package kotlin_stream_bq

import org.json.JSONArray
import org.json.JSONObject
import java.util.*


class AppValues {

    companion object{

        val PROJECT_ID = "..."
        val DATASET = "..."
        val TABLE = "..."

        private fun getText():String =
            this::class.java.classLoader.getResource("sampleText.txt").readText()

        fun getJsonData(amount:Int): JSONArray {
            // Create a JSON object that is compatible with the table schema.
            val jsonArr = JSONArray()
            for (j in 1..amount) {
                val record = JSONObject()
                record.put("text", UUID.randomUUID().toString())
                record.put("data", getText())
                jsonArr.put(record)
            }

            return jsonArr
        }
    }
}