package kotlin_stream_bq

import com.google.cloud.bigquery.storage.v1.*
import org.json.JSONArray
import java.util.concurrent.TimeUnit

class BQWriterV2{

    private val parent: String =
        "projects/${AppValues.PROJECT_ID}/datasets/${AppValues.DATASET}/tables/${AppValues.TABLE}"

    fun executeBQStreamWithParallelStream(jsonArrays: List<JSONArray>){

        val writeStream = getWriteStream()

        jsonArrays.parallelStream().forEach {
            appendData(getJsonStreamWriter(writeStream),it)
        }

    }

    fun executeBQStreamWithTwoJsonArraysSequentially(jsonArrays: List<JSONArray>){

        val writeStream = getWriteStream()

        jsonArrays.forEach {
            appendData(getJsonStreamWriter(writeStream), it)
        }

    }

    private fun getWriteStream():WriteStream =
        BigQueryWriteClient.create()
        .createWriteStream(
            CreateWriteStreamRequest.newBuilder()
                .setParent(parent)
                .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build())
                .build())

    private fun getJsonStreamWriter(writeStream: WriteStream):JsonStreamWriter =
        JsonStreamWriter.newBuilder(parent,
            writeStream.tableSchema)
            .build()

    private fun appendData(jsonStreamWriter: JsonStreamWriter,jsonArray: JSONArray) =
        jsonStreamWriter.use { writer ->

            writer.append(jsonArray).get(60000 / 2, TimeUnit.SECONDS).let { appendRowsResponse ->
                if (appendRowsResponse.hasError()) {
                    throw Exception("failure when inserting messages in BigQuery: ${appendRowsResponse.error.message}")
                }
            }
        }

}