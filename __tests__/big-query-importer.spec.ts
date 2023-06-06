import {
  BigQueryImporter,
  BigQueryImporterOptions,
} from "../src/big-query-importer";
import dotenv from "dotenv";
dotenv.config({ path: ".env.test" });
const options: BigQueryImporterOptions = {
  projectId: process.env["BIGQUERY_PROJECT_ID"] || "<YOUR_BIGQUERY_PROJECT_ID>",
  datasetId: process.env["BIGQUERY_DATASET_ID"] || "<YOUR_BIGQUERY_DATASET_ID>",
  location: process.env["BIGQUERY_LOCATION"] || "<YOU BIGQUERY LOCATION>",
};

test("constructor", async () => {
  const importer = new BigQueryImporter(options);
  expect(importer["options"]).toEqual(options);
});

test("importAvroToBigQuery", async () => {
  const importer = new BigQueryImporter(options);
  const result = await importer.importAvroToBigQuery(
    `${__dirname}/fixtures/People.avro`,
    `testImportAvroToBigQuery`
  );
  expect(result).toMatchObject({
    table: `${options.projectId}.${options.datasetId}.testImportAvroToBigQuery`,
    jobId: expect.stringMatching(
      new RegExp(`${options.projectId}:${options.location}\.(.*)`)
    ),
  });
});
