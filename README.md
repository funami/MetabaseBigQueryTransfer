# MetabaseBigQueryTransfer

Transfer Metabase Questions result to BigQuery table

# 概要

metabase の 質問(card) id を指定して、export し、そのデータを Bigquery のテーブルとして import する

# セットアップ

## インストール

```
git clone git@github.com:funami/MetabaseBigQueryTransfer.git
```

pull したリポジトリのルートに cd して

```
npm install
npm run build
```

## metabase と bigquery の設定

package.json と同階層に、`.env`ファイルを作成し、以下環境を書き込む

```sh
GOOGLE_APPLICATION_CREDENTIALS=/Users/test/.ssh/gcloud/forbigquery.json
METABASE_USERNAME=<metabase username>
METABASE_PASSWORD=<metabase password>
METABASE_API_ENDPOINT=<metabase api host>/api/
METABASE_EXPORT_TEMP_DIR=/tmp/metabase-export-temp
BIGQUERY_PROJECT_ID=<bigquery project id>
BIGQUERY_DATASET_ID=<bigquery dataset id>
BIGQUERY_LOCATION=<bigquery location>
```

例:

```sh
GOOGLE_APPLICATION_CREDENTIALS=/Users/test/.ssh/gcloud/forbigquery.json
METABASE_USERNAME=myokya972@fuwari.be
METABASE_PASSWORD=myokya972pass
METABASE_API_ENDPOINT=http://localhost:3000/api/
METABASE_EXPORT_TEMP_DIR=/tmp/metabase-export-temp
BIGQUERY_PROJECT_ID=testbq
BIGQUERY_DATASET_ID=datalake
BIGQUERY_LOCATION=asia-northeast1
```

## 対象となる、metabase の質問設定

sample/config.json を参考に、問い合わせる質問(card)ID と parameters を設定
parameters は、省略可能
質問のレスポンスフィールド名が英数字以外のときは、schema_field_name_index にて、名前の変換設定を行う
bigquery_table_sufix(省略可能)を指定すると、bigquery のテーブル名の後ろに "\*" + bigquery_table_sufix が付与される

### 例

以下のような SQL で書かれた question(質問)があって、questsion の id=card id が 2 の場合

```sql
SELECT
    ID, TITLE AS タイトル, CATEGORY AS カテゴリ
FROM products
WHERE id = {{id_num}}
```

![](doc/images/question_sample.png)

```json
{
  "card_id": 2,
  "parameters": [
    {
      "id": "3a703c96-0d2b-cf0e-894e-70af0647cac8",
      "type": "category",
      "value": "3",
      "target": ["variable", ["template-tag", "id_num"]]
    }
  ],
  "schema_field_name_index": {
    "タイトル": "title",
    "カテゴリ": "category"
  },
  "bigquery_table_suffix": "2023"
}
```

## 実行

```
cd <リポジトリルート=package.jsonと同階層>
node ./dist/metabase-bigquery-transfer.js -c config/sample.json
```

しばし待つ

- metabase export sucess
- bigquery import sucess  
  のログが表示されれば成功

例:

```
metabase export sucess  {
  "schema": {
    "type": "record",
    "name": "metabase_card_2_hello",
    "fields": [
      {
        "name": "ID",
        "type": "long",
        "doc": "ID"
      },
      {
        "name": "title",
        "type": "string",
        "doc": "タイトル"
      },
      {
        "name": "category",
        "type": "string",
        "doc": "カテゴリ"
      }
    ]
  },
  "outAvroFile": "/tmp/metabase-temp/metabase_card_2_hello.avro",
  "length": 1
}
bigquery import sucess  {
  "table": "testbq.datalake.metabase_card_2_hello_2023",
  "jobId": "testbq:asia-northeast1.4c0e137e-17a7-4078-a42c-6d723145d7ab"
}
```

# test

## 準備

### metabase

- metabase test 環境として、Docker にて、サンプルのデータ取得用の環境を用意する
  - 参照: https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker

```
docker pull metabase/metabase:latest
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

### bigquery

テスト用の project を用意し、データセットを作成しておく(or 作成権限のある、サービスアカウントなら、test から自動生成可能)
テスト用のデータセットにてテーブル作成権限のある、サービスアカウントを用意し、クレデンシャルのパスを GOOGLE_APPLICATION_CREDENTIALS にセット

```
export GOOGLE_APPLICATION_CREDENTIALS=~/.ssh/gcloud/<サービスアカウント鍵ファイル>.json
```

### .env.test の設定

GOOGLE_APPLICATION_CREDENTIALS には、bigquery.jobs.create が必要。
参照: https://cloud.google.com/bigquery/docs/jobs-overview

```
GOOGLE_APPLICATION_CREDENTIALS=/Users/test/.ssh/gcloud/forbigquery.json
METABASE_USERNAME=myokya972@fuwari.be
METABASE_PASSWORD=myokya972pass
METABASE_API_ENDPOINT=http://localhost:3000/api/
METABASE_EXPORT_TEMP_DIR=/tmp/metabase-export-temp
BIGQUERY_PROJECT_ID=testbq
BIGQUERY_DATASET_ID=datalake
BIGQUERY_LOCATION=asia-northeast1
```
