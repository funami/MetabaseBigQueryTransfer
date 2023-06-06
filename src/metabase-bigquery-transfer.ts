import { MetabaseClient } from "./metabase-client";
import { BigQueryImporter } from "./big-query-importer";
import { program } from "commander";
import fs from "fs-extra";
import path from "path";
import dotenv from "dotenv";

/**
 * parameters の例
 * -c 2 -p [{"id":"3a703c96-0d2b-cf0e-894e-70af0647cac8","type":"category","value":"2","target":["variable",["template-tag","id_num"]]}]
 */
program
  .requiredOption(
    "-c, --config <char>",
    "metabase question 設定ファイル.このファイルからの相対パス"
  )
  .option("-e --env_name <char>", "環境名")
  .option(
    "--use_cache",
    "metabaeから取得したjsonをキャッシュとして使うかどうか"
  );

program.parse();
const options = program.opts();

const env = dotenv.config({
  path: options.env_name ? `.env.${options.env_name}` : `.env`,
});
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS;
const username = process.env.METABASE_USERNAME;
const password = process.env.METABASE_PASSWORD;
const apiEndpoint = process.env.METABASE_API_ENDPOINT;
const projectId = process.env.BIGQUERY_PROJECT_ID;
const datasetId = process.env.BIGQUERY_DATASET_ID;
const location = process.env.BIGQUERY_LOCATION;

const config = fs.readJSONSync(path.join(__dirname, "..", options.config));
const cardId = parseInt(config.card_id);
const parameters = JSON.stringify(config.parameters);
const schemaFieldNameIndex = config.schema_field_name_index;
const useCache = options.use_cache ? true : false;
console.log({
  GOOGLE_APPLICATION_CREDENTIALS,
  username,
  password,
  apiEndpoint,
  projectId,
  datasetId,
  location,
  cardId,
  parameters: parameters,
  useCache,
  schemaFieldNameIndex,
});
if (
  !username ||
  !password ||
  !apiEndpoint ||
  !projectId ||
  !datasetId ||
  !location ||
  !cardId
) {
  throw new Error(
    `username or password or apiEndpoint or projectId or datasetId or location or cardId is not set (${JSON.stringify(
      {
        username,
        password,
        apiEndpoint,
        projectId,
        datasetId,
        location,
        cardId,
      }
    )})`
  );
}

const mb = new MetabaseClient({
  username,
  password,
  apiEndpoint,
});

const bq = new BigQueryImporter({
  projectId,
  datasetId,
  location,
});
const main = async () => {
  const res = await mb.cardExport(cardId, parameters, {
    useCache,
    schemaFieldNameIndex,
  });

  return res;
};

main()
  .then(async (result) => {
    console.log("metabase export sucess ", JSON.stringify(result, null, 2));
    return bq.importAvroToBigQuery(result.outAvroFile, result.schema.name);
  })
  .then((result) => {
    console.log("bigquery import sucess ", JSON.stringify(result, null, 2));
    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
