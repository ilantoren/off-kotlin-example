package mongolovesdata

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import com.mongodb.reactivestreams.client.MongoCollection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.runBlocking
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.pojo.PojoCodecProvider

/**
 *   Based on the monngodb dump for OpenFoodFacts jan 2022
 *   Find the records from the usda and save them into a new collection
 */
class OffKotlin {

    data class Usda (
        var _id:  String = "",
        var product_name: String  = "",
        var url: String = ""
    )


    // The registry uses the Usda data class to serialize the results of the aggregation query
    val pojoCodecRegistry: CodecRegistry = CodecRegistries.fromRegistries(
        MongoClients.getDefaultCodecRegistry(),
        CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
    )



    // see  mongolovesdata.medium.com
    private val pipeline = listOf(
        Document(
            "\$match",
            Document(
                "\$and", listOf(
                    Document(
                        "creator",
                        Document("\$regex", "usda")
                            .append("\$options", "i")
                    ),
                    Document(
                        "sources.url",
                        Document("\$exists", 1L)
                    )
                )
            )
        ),
        Document(
            "\$project",
            Document("_id", 1L)
                .append("url", "\$sources.url")
                .append("product_name", 1L)
        ),
        Document(
            "\$unwind",
            Document("path", "\$url")
                .append("includeArrayIndex", "ind")
                .append("preserveNullAndEmptyArrays", false)
        ),
        Document(
            "\$addFields",
            Document(
                "isValid",
                Document("\$eq", listOf(Document("\$type", "\$url"), "string"))
            )
        ),
        Document(
            "\$match",
            Document("ind", 0L)
                .append("isValid", true)
        )
    )


    fun run()  = runBlocking {
        val connectionString = ConnectionString("mongodb://localhost:27017/off")
        val settings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .build()
        val mongoClient = MongoClients.create(settings)
        launch {
            listCollections(mongoClient)
            getOff(mongoClient).collect{
               persist( mongoClient, it)
            }
        }
    }

    private suspend fun persist(mongoClient: MongoClient?, doc: OffKotlin.Usda) {
         mongoClient?.let {client ->
            val col :MongoCollection<Usda> =  client.getDatabase(DB).withCodecRegistry(pojoCodecRegistry).getCollection(COLLECTION, Usda::class.java)
             col.insertOne(doc).asFlow().collect{
                 log.info( "Insert ${it.insertedId}  $doc")
             }
         }

    }

    private suspend  fun listCollections(client: MongoClient) {
        client.getDatabase(DB).listCollections().collect {
            println( it )
        }
    }

     private fun getOff( client: MongoClient ): Flow<Usda> = flow {
       val collection: MongoCollection<Usda> = client.getDatabase(DB).withCodecRegistry(pojoCodecRegistry).getCollection("products", Usda::class.java)
          collection.aggregate(pipeline).batchSize(1000).asFlow().collect {
              emit( it )
          }
    }

    companion object {

        val log: Logger = LogManager.getLogger("off")
        const val DB = "off"
        const val COLLECTION = "usda2"

        @JvmStatic
        fun main( args: Array<String>) {
            OffKotlin().run()
        }
    }
}