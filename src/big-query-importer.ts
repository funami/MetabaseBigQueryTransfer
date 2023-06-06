import { BigQuery } from "@google-cloud/bigquery";

export type BigQueryImporterOptions = {
  projectId: string;
  datasetId: string;
  location: string;
};

export class BigQueryImporter {
  constructor(private options: BigQueryImporterOptions) {}

  /** arvo file を BigQueryにimportする
   * @param avroFilePath
   */
  async importAvroToBigQuery(avroFilePath: string, tableName: string) {
    const { projectId, datasetId, location } = this.options;
    // console.log({ projectId, datasetId, location });
    const bigquery = new BigQuery({ projectId, location });
    const dataset = bigquery.dataset(datasetId);
    const [datasetExists] = await dataset.exists();

    if (!datasetExists) {
      console.log("DATASET IS NOT EXISTS, SO CREATE", datasetId);
      await dataset.create();
    }

    const table = dataset.table(tableName);
    const [exists] = await table.exists();
    if (!exists) {
      await table.create();
    }
    const [job] = await table.load(avroFilePath, {
      sourceFormat: "AVRO",
      writeDisposition: "WRITE_TRUNCATE",
      useAvroLogicalTypes: true,
    });
    // console.log(`Job ${job.id} completed.`);
    const errors = job.status?.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }
    return { table: [projectId, datasetId, table.id].join("."), jobId: job.id };
  }
}
