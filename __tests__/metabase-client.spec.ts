import { MetabaseClient, cache } from "../src/metabase-client";
import MockAdapter from "axios-mock-adapter";
import axios from "axios";
import avro from "avsc";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import fs from "fs-extra";
dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.tz.setDefault("Asia/Tokyo");

const mock = new MockAdapter(axios);

const client = new MetabaseClient({
  username: "test@test.com",
  password: "O2hqk582BJQ6lU",
  apiEndpoint: "http://localhost:3000/api/",
  metabaseTempDir: `${__dirname}/tmp`,
});
test("constructor", () => {
  expect(client["options"]).toEqual({
    username: "test@test.com",
    password: "O2hqk582BJQ6lU",
    apiEndpoint: "http://localhost:3000/api/",
    metabaseTempDir: `${__dirname}/tmp`,
  });
});
test("sessionIdCacheKey", () => {
  expect(client.sessionIdCacheKey()).toEqual(
    "b9cb05bc2a1fa1f41803c07be33dfd03"
  );
});

describe("session", () => {
  beforeEach(() => {
    mock.onPost("http://localhost:3000/api/session").reply(200, {
      id: "1234-5678-9012-3456",
    });
  });
  afterEach(async () => {
    mock.reset();
  });
  test("when this.sessionId and cache is undefined, get from metabase", async () => {
    client.sessionId = undefined;
    await cache.remove(client.sessionIdCacheKey());
    const sessionId = await client.session();
    expect(sessionId).toEqual("1234-5678-9012-3456");
    expect(client.sessionId).toEqual("1234-5678-9012-3456");
    expect(mock.history.post.length).toBe(1);
  });
  test("when this.sessionId is undefined, but cache is set, return cache", async () => {
    client.sessionId = undefined;
    await cache.set(client.sessionIdCacheKey(), "1234");
    const sessionId = await client.session();
    expect(sessionId).toEqual("1234");
    expect(client.sessionId).toEqual("1234");
    expect(mock.history.post.length).toBe(0);
  });
  test("when this.sessionId is not undefined, return this.sessionId", async () => {
    client.sessionId = "2222";
    await cache.set(client.sessionIdCacheKey(), "1234");
    const sessionId = await client.session();
    expect(sessionId).toEqual("2222");
    expect(client.sessionId).toEqual("2222");
    expect(mock.history.post.length).toBe(0);
  });
  test("when this.sessionId is set, refresh, return from metabase", async () => {
    client.sessionId = "2222";
    await cache.set(client.sessionIdCacheKey(), "2222");
    const sessionId = await client.session({ refresh: true });
    expect(sessionId).toEqual("1234-5678-9012-3456");
    expect(client.sessionId).toEqual("1234-5678-9012-3456");
    expect(mock.history.post.length).toBe(1);
  });
});

describe("apiRequest", () => {
  beforeEach(() => {
    mock.onPost("http://localhost:3000/api/session").reply(200, {
      id: "1234-5678-9012-3456",
    });
    mock
      .onPost("http://localhost:3000/api/card/100/query/json")
      .reply((config) => {
        // console.log(config);
        if (
          (config.headers || {})["X-Metabase-Session"] !== "1234-5678-9012-3456"
        ) {
          return [401, "Unauthorized"];
        }
        return [
          200,
          [
            {
              customer_id: "9999",
              customer_scale: "デスク",
              comp_name: "確認用テストアカウント",
              created_at: "2019-06-01",
            },
            {
              customer_id: "10001",
              customer_scale: null,
              comp_name: "東京オフィス",
              created_at: "2019-06-01",
            },
          ],
        ];
      });
  });
  afterEach(async () => {
    mock.reset();
  });
  test("when this.sessionId and cache is undefined, get from metabase", async () => {
    client.sessionId = undefined;
    await cache.remove(client.sessionIdCacheKey());
    const res = await client.apiRequest({
      method: "post",
      url: "http://localhost:3000/api/card/100/query/json",
    });
    expect(client.sessionId).toEqual("1234-5678-9012-3456");
    expect(mock.history.post.length).toBe(2);
  });
  test("when this.sessionId is undefined, but cache is set", async () => {
    client.sessionId = undefined;
    await cache.set(client.sessionIdCacheKey(), "1234-5678-9012-3456");
    const res = await client.apiRequest({
      method: "post",
      url: "http://localhost:3000/api/card/100/query/json",
    });
    expect(client.sessionId).toEqual("1234-5678-9012-3456");
    expect(mock.history.post.length).toBe(1);
  });
  test("when this.sessionId is set", async () => {
    client.sessionId = "1234-5678-9012-3456";
    await cache.set(client.sessionIdCacheKey(), "1234");
    const res = await client.apiRequest({
      method: "post",
      url: "http://localhost:3000/api/card/100/query/json",
    });
    expect(client.sessionId).toEqual("1234-5678-9012-3456");
    expect(mock.history.post.length).toBe(1);
  });
  test("when this.sessionId is expired, refresh sessionId", async () => {
    client.sessionId = "2222";
    await cache.set(client.sessionIdCacheKey(), "2222");
    const res = await client.apiRequest({
      method: "post",
      url: "http://localhost:3000/api/card/100/query/json",
    });
    expect(client.sessionId).toEqual("1234-5678-9012-3456");
    expect(mock.history.post.length).toBe(3);
  });
});

describe("cardSchema", () => {
  beforeEach(() => {
    const testSchema = fs.readJSONSync(
      `${__dirname}/fixtures/result_metadata.json`
    );
    mock.onGet("http://localhost:3000/api/card/1").reply((config) => {
      return [200, testSchema];
    });
  });
  afterEach(async () => {
    mock.reset();
  });
  test("cardSchema", async () => {
    const res = await client.cardSchema(1);
    expect(res).toEqual({
      type: "record",
      name: "People",
      fields: [
        { name: "ID", type: "long", doc: "ID" },
        { name: "ADDRESS", type: "string", doc: "Address" },
        { name: "EMAIL", type: "string", doc: "Email" },
        { name: "PASSWORD", type: "string", doc: "Password" },
        { name: "NAME", type: "string", doc: "Name" },
        { name: "CITY", type: "string", doc: "City" },
        { name: "LONGITUDE", type: "double", doc: "Longitude" },
        { name: "STATE", type: "string", doc: "State" },
        { name: "SOURCE", type: "string", doc: "Source" },
        {
          name: "BIRTH_DATE",
          type: { type: "long", logicalType: "timestamp-micros" },
          doc: "Birth Date",
        },
        { name: "ZIP", type: "string", doc: "Zip" },
        { name: "LATITUDE", type: "double", doc: "Latitude" },
        {
          name: "CREATED_AT",
          type: { type: "long", logicalType: "timestamp-micros" },
          doc: "Created At",
        },
      ],
    });
  });
});

describe("cardExport", () => {
  beforeEach(() => {
    const testSchema = fs.readJSONSync(
      `${__dirname}/fixtures/result_metadata.json`
    );
    const testCardData = fs.readJSONSync(`${__dirname}/fixtures/1.json`);
    mock.onGet("http://localhost:3000/api/card/1").reply((config) => {
      return [200, testSchema];
    });
    mock
      .onPost("http://localhost:3000/api/card/1/query/json")
      .reply((config) => {
        return [200, testCardData];
      });
    const testSchema2 = fs.readJSONSync(`${__dirname}/fixtures/2_query.json`);
    const testCardData2 = fs.readJSONSync(`${__dirname}/fixtures/2.json`);
    mock.onGet("http://localhost:3000/api/card/2").reply((config) => {
      return [200, testSchema2];
    });
    mock
      .onPost("http://localhost:3000/api/card/2/query/json")
      .reply((config) => {
        return [200, testCardData2];
      });
  });
  afterEach(async () => {
    mock.reset();
  });

  test("cardExport", async () => {
    const res = await client.cardExport(1);
    // console.log(JSON.stringify(res.schema));
    expect(res.schema).toEqual({
      type: "record",
      name: "metabase_card_1_People",
      fields: [
        { name: "ID", type: "long", doc: "ID" },
        { name: "ADDRESS", type: "string", doc: "Address" },
        { name: "EMAIL", type: "string", doc: "Email" },
        { name: "PASSWORD", type: "string", doc: "Password" },
        { name: "NAME", type: "string", doc: "Name" },
        { name: "CITY", type: "string", doc: "City" },
        { name: "LONGITUDE", type: "double", doc: "Longitude" },
        { name: "STATE", type: "string", doc: "State" },
        { name: "SOURCE", type: "string", doc: "Source" },
        {
          name: "BIRTH_DATE",
          type: { type: "long", logicalType: "timestamp-micros" },
          doc: "Birth Date",
        },
        { name: "ZIP", type: "string", doc: "Zip" },
        { name: "LATITUDE", type: "double", doc: "Latitude" },
        {
          name: "CREATED_AT",
          type: { type: "long", logicalType: "timestamp-micros" },
          doc: "Created At",
        },
      ],
    });
    expect(res.outAvroFile).toMatch(
      /\/__tests__\/tmp\/metabase_card_1_People.avro$/
    );
    const rows = await new Promise<{ ID: number; [key: string]: any }[]>(
      (resolve, reject) => {
        const rows: any = [];
        const decoder = avro
          .createFileDecoder(res.outAvroFile)
          .on("metadata", function (type) {
            /* `type` is the writer's type. */
          })
          .on("data", function (record) {
            rows.push(record);
          })
          .on("end", function () {
            resolve(rows);
          });
      }
    );
    const row = rows.find((row) => row["ID"] == 1);
    if (!row) throw new Error("row is not found");

    const rowValue: Record<string, any> = {};
    Object.keys(row).forEach((key) => {
      const fieldSchema = res.schema.fields.find((field) => field.name == key);
      let value = row[key];
      if ((fieldSchema?.type as any)["logicalType"] == "timestamp-micros") {
        const fmt =
          key == "BIRTH_DATE" ? "YYYY-MM-DD" : "YYYY-MM-DDTHH:mm:ss.SSS";
        value = dayjs(row[key] / 1000)
          .utc()
          .tz("Asia/Tokyo")
          .format(fmt);
      }
      if (fieldSchema) rowValue[fieldSchema.doc || fieldSchema.name] = value;
    });
    expect(rows.length).toEqual(2500);
    expect(rowValue).toEqual({
      Address: "9611-9809 West Rosedale Road",
      Longitude: -98.5259864,
      Email: "borer-hudson@yahoo.com",
      "Created At": "2017-10-07T01:34:35.462",
      Password: "ccca881f-3e4b-4e5c-8336-354103604af6",
      Source: "Twitter",
      ID: 1,
      Zip: "68883",
      Latitude: 40.71314890000001,
      "Birth Date": "1986-12-12",
      City: "Wood River",
      State: "NE",
      Name: "Hudson Borer",
    });
  });
  test("cardExport with parameters", async () => {
    // mock.restore();
    const res = await client.cardExport(
      2,
      '[{"id":"3a703c96-0d2b-cf0e-894e-70af0647cac8","type":"category","value":["2"],"target":["variable",["template-tag","id_num"]]}]'
    );
    // console.log("SCHEMA", JSON.stringify(res.schema));
    expect(res.schema).toEqual({
      type: "record",
      name: "metabase_card_2_hello",
      fields: [
        { name: "ID", type: "long", doc: "ID" },
        { name: "EAN", type: "string", doc: "EAN" },
        { name: "TITLE", type: "string", doc: "TITLE" },
        { name: "CATEGORY", type: "string", doc: "CATEGORY" },
        { name: "VENDOR", type: "string", doc: "VENDOR" },
        { name: "PRICE", type: "double", doc: "PRICE" },
        { name: "RATING", type: "double", doc: "RATING" },
        {
          name: "CREATED_AT",
          type: { type: "long", logicalType: "timestamp-micros" },
          doc: "CREATED_AT",
        },
      ],
    });
    expect(res.outAvroFile).toMatch(
      /\/__tests__\/tmp\/metabase_card_2_hello.avro$/
    );

    const rows = await new Promise<{ ID: number; [key: string]: any }[]>(
      (resolve, reject) => {
        const rows: any = [];
        const decoder = avro
          .createFileDecoder(res.outAvroFile)
          .on("metadata", function (type) {
            /* `type` is the writer's type. */
          })
          .on("data", function (record) {
            rows.push(record);
          })
          .on("end", function () {
            resolve(rows);
          });
      }
    );
    // console.log(rows);
    const row = rows.find((row) => row["ID"] == 2);
    if (!row) throw new Error("row is not found");

    const rowValue: Record<string, any> = {};
    Object.keys(row).forEach((key) => {
      const fieldSchema = res.schema.fields.find((field) => field.name == key);
      let value = row[key];
      if ((fieldSchema?.type as any)["logicalType"] == "timestamp-micros") {
        const fmt = "YYYY-MM-DDTHH:mm:ss.SSS";
        value = dayjs(row[key] / 1000)
          .utc()
          .tz("Asia/Tokyo")
          .format(fmt);
      }
      if (fieldSchema) rowValue[fieldSchema.doc || fieldSchema.name] = value;
    });
    expect(rows.length).toEqual(1);
    // console.log("rows", rows);
    expect(rowValue).toEqual({
      ID: 2,
      EAN: "7663515285824",
      TITLE: "Small Marble Shoes",
      CATEGORY: "Doohickey",
      VENDOR: "Balistreri-Ankunding",
      PRICE: 70.07989613071763,
      RATING: 0.0,
      CREATED_AT: "2019-04-11T08:49:35.932",
    });
  });
});
