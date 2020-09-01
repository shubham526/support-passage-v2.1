package help;
import org.apache.lucene.document.Document;
import org.jetbrains.annotations.Contract;

import java.util.ArrayList;

/**
 * Class to represent an Entity Context Document for an entity.
 * @author Shubham Chatterjee
 * @version 05/31/2020
 */
public class EntityContextDocument {

    /**
     * Inner class to represent an entity in context.
     */
    public static class ContextEntity {
        private final String entityID;
        private final String anchorText;

        public ContextEntity(String entityID, String anchorText) {
            this.entityID = entityID;
            this.anchorText = anchorText;
        }

        @Override
        public String toString() {
            return "ContextEntity{" +
                    "entityID='" + entityID + '\'' +
                    ", anchorText='" + anchorText + '\'' +
                    '}';
        }

        public String getEntityID() {
            return entityID;
        }

        public String getAnchorText() {
            return anchorText;
        }
    }

    private final ArrayList<Document> documentList;
    private final String entity;
    private final ArrayList<ContextEntity> contextEntities;

    /**
     * Construcor.
     * @param documentList List of documents in the pseudo-document
     * @param entity The entity for which the pseudo-document is made
     * @param contextEntities The list of entities in the pseudo-document
     */
    @Contract(pure = true)
    public EntityContextDocument(ArrayList<Document> documentList, String entity,
                                 ArrayList<ContextEntity> contextEntities) {
        this.documentList = documentList;
        this.entity = entity;
        this.contextEntities = contextEntities;
    }

    /**
     * Method to get the list of documents in the ECD.
     * @return String
     */
    public ArrayList<Document> getDocumentList() {
        return this.documentList;
    }

    /**
     * Method to get the entity of the ECD.
     * @return String
     */
    public String getEntity() {
        return this.entity;
    }

    /**
     * Method to get the list of context entities in the ECD.
     * @return ArrayList
     */
    public ArrayList<ContextEntity> getEntityList() {
        return this.contextEntities;
    }
}

