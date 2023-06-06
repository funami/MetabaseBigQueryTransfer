import axios, { AxiosError } from "axios";
import qs from "qs";
import * as url from "url";
import path from "path";
import crypto from "crypto";
import Cache from "file-system-cache";
import avro from "avsc";
import fs from "fs-extra";
import Bluebird, { reject } from "bluebird";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import { deepmerge } from "deepmerge-ts";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.tz.setDefault("Asia/Tokyo");
export const cache = Cache({ ttl: 3600 * 24 * 14 });

export type MetabaseClientOptions = {
  username: string;
  password: string;
  apiEndpoint: string;
  metabaseTempDir?: string;
};
export class MetabaseClient {
  public sessionId?: string;

  constructor(private options: MetabaseClientOptions) {}

  get metabaseTempDir(): string {
    const tmpPath = this.options.metabaseTempDir || "/tmp/metabase-temp";
    fs.mkdirpSync(tmpPath);
    return tmpPath;
  }
  sessionIdCacheKey = (): string => {
    const md5 = crypto.createHash("md5");
    const id = `${this.options.username}-${this.options.apiEndpoint}-sessionId`;
    return md5.update(id, "binary").digest("hex");
  };
  /**
   * Get session id
   * インスタンス or キャッシュにセッションIDがあれば、そのセッションIDを返す。
   * なければ、APIを叩いてセッションIDを取得する。
   */
  async session(options: { refresh?: boolean } = {}): Promise<string> {
    if (options.refresh) {
      this.sessionId = undefined;
      await cache.remove(this.sessionIdCacheKey());
    }
    if (!this.sessionId) {
      this.sessionId = await cache.get(this.sessionIdCacheKey());
    }
    if (this.sessionId) return this.sessionId;
    return new Promise(async (resolve, reject) => {
      const data = JSON.stringify({
        username: this.options.username,
        password: this.options.password,
      });
      const headers = {
        "Content-Type": "application/json",
        "User-Agent": "MetabaseBigQueryTransfer/0.0.1",
      };
      const config = {
        method: "post",
        maxBodyLength: Infinity,
        url: url.resolve(this.options.apiEndpoint, "session"),
        headers,
        data,
      };
      try {
        const response = await axios(config);
        this.sessionId = response.data.id;
        if (this.sessionId) {
          const filepath = await cache.set(
            this.sessionIdCacheKey(),
            this.sessionId
          );
          // console.log("filepath", filepath);
          resolve(this.sessionId);
        } else {
          await cache.remove(this.sessionIdCacheKey());
          reject(new Error("Failed to get session id"));
        }
      } catch (e) {
        reject(e);
      }
    });
  }

  /**
   * metabase api を呼び出す
   * sessionIdが無効の場合は、sessionを1回だけ取得しなおす
   * @param config
   * @returns
   */
  async apiRequest(config: {
    method: string;
    url: string;
    headers?: Record<string, string>;
    data?: string;
  }) {
    await this.session();
    const _config = () => {
      return deepmerge(
        {
          maxBodyLength: Infinity,
          headers: {
            "Content-Type": "application/json",
            "X-Metabase-Session": this.sessionId,
          },
        },
        config
      );
    };
    // console.log("config", _config());
    const c = _config();
    return axios.request(c).catch(async (e: AxiosError) => {
      if (e.response?.status == 401) {
        await this.session({ refresh: true });
      }
      const c = _config();
      return axios.request(c);
    });
  }

  /**
   * card_id に対応する metabase のカードのschemaを作成する
   * @param card_id
   * @returns
   */
  async cardSchema(
    card_id: number,
    schemaFieldNameIndex?: Record<string, string>
  ): Promise<avro.schema.RecordType> {
    const config = {
      method: "get",
      url: url.resolve(
        this.options.apiEndpoint,
        path.join("card", card_id.toString())
      ),
    };
    const res = await this.apiRequest(config).then((response) => {
      return response.data;
    });
    const fields = res.result_metadata?.map((item: any) => {
      const name =
        (schemaFieldNameIndex && schemaFieldNameIndex[item.name]) || item.name;
      return {
        name,
        type: this.typeConverter(item),
        doc: item.display_name,
      };
    });
    const schema: avro.schema.RecordType = {
      type: "record",
      name: res.name,
      fields,
    };
    fs.writeJSONSync(
      path.join(this.metabaseTempDir, `${card_id}.scema.json`),
      schema
    );
    return schema;
  }

  typeConverter(item: Record<string, any>): avro.schema.AvroSchema {
    const typeIndex: Record<string, any> = {
      Text: "string",
      DateTime: {
        type: "long",
        logicalType: "timestamp-micros",
      },
      Date: {
        type: "long",
        logicalType: "timestamp-micros",
      },
      Integer: "int",
      Float: "double",
      Decimal: "long",
      BigInteger: "long",
    };
    const typeName = item.base_type.split("/")[1];
    const _type = typeIndex[typeName];
    if (!_type) throw new Error(`Type ${typeName} is not supported`);
    const nilRate = (item.fingerprint?.global || {})["nil%"] || 0;
    return nilRate > 0 ? ["null", _type] : _type;
  }

  async cardExport(
    card_id: number,
    parameters?: string,
    options?: {
      useCache?: boolean;
      length?: number;
      schemaFieldNameIndex?: Record<string, string>;
    }
  ): Promise<{
    schema: avro.schema.RecordType;
    outAvroFile: string;
    length: number;
  }> {
    const useCache = options?.useCache || false;
    const schema = await this.cardSchema(
      card_id,
      options?.schemaFieldNameIndex
    );
    const data = parameters
      ? qs.stringify({
          parameters,
        })
      : undefined;
    let rows =
      useCache &&
      fs.existsSync(path.join(this.metabaseTempDir, `${card_id}.json`))
        ? fs.readJSONSync(path.join(this.metabaseTempDir, `${card_id}.json`))
        : null;
    if (!rows) {
      rows = await this.apiRequest({
        method: "post",
        url: url.resolve(
          this.options.apiEndpoint,
          path.join("card", card_id.toString(), "query", "json")
        ),
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        data,
      }).then((response) => {
        return response.data;
      });
      fs.writeFileSync(
        path.join(this.metabaseTempDir, `${card_id}.json`),
        JSON.stringify(rows, null, 2)
      );
    }
    schema.name = `metabase_card_${card_id}_${schema.name.replace(
      /[^0-9a-z]/gi,
      ""
    )}`;
    if (options?.length) rows = rows.slice(0, options?.length);
    const outAvroFile = await this.writeAvroFile(schema, rows);
    return { schema, outAvroFile, length: rows.length };
  }

  async writeAvroFile(
    schema: avro.schema.RecordType,
    rows: Record<string, any>[]
  ): Promise<string> {
    const outfilePath = path.join(this.metabaseTempDir, `${schema.name}.avro`);
    try {
      const encoder = avro.createFileEncoder(outfilePath, schema);
      const maxConcurrent = 100; // 並列実行するタスク数
      return new Promise((resolve, reject) => {
        Bluebird.map(
          rows,
          (_row, i) => {
            const row: Record<string, any> = {};
            Object.keys(_row).forEach((key) => {
              const name = schema.fields.find(
                (field) => field.doc == key
              )?.name;
              if (name) row[name] = _row[key];
            });
            const data: Record<string, any> = {};
            schema.fields.forEach((field) => {
              const name = field.name;
              if (field.type == "string") {
                data[name] = `${row[name]}`;
              } else if (field.type.valueOf().hasOwnProperty("logicalType")) {
                data[name] = row[name]
                  ? dayjs.tz(row[name]).valueOf() * 1000
                  : row[name];
              } else if (field.type == "int" && row[name] == null) {
                data[name] = 0;
              } else {
                data[name] = row[name];
              }
            });
            try {
              encoder.write(data, (err) => {
                if (err) reject(err);
              });
            } catch (e) {
              console.log("ERROR!!!", row, data, e);
            }

            return;
          },
          { concurrency: maxConcurrent }
        )
          .then(() => {
            encoder.end(() => {
              resolve(outfilePath);
            });
          })
          .catch((err) => {
            reject(err);
          });
      });
    } catch (e) {
      if (e instanceof Error) {
        if (e.message.match("invalid field name")) {
          return Promise.reject(
            new Error(
              `フィールド名として使えない文字があります。config.schema_field_name_indexにて、変換ルールを指定してください. ${e.message}`
            )
          );
        }
      }
      return Promise.reject(e);
    }
  }
}
