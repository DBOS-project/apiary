package org.dbos.apiary.mongo;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

public class MongoUtilities {
    public static void prettyPrint(Document document) {
        BsonDocument b = document.toBsonDocument(BsonDocument.class, Bson.DEFAULT_CODEC_REGISTRY);
        JsonWriterSettings.Builder settingsBuilder = JsonWriterSettings.builder().indent(true);
        System.out.println(b.toJson(settingsBuilder.build()));
    }
}
