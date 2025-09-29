package edu.utexas.tacc.tapis.jobs.model;

import java.util.TreeSet;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public class JobAnnotation {
    public static final JsonObject EMPTY_JSON_OBJ = TapisGsonUtils.getGson().fromJson("{}", JsonObject.class);

    private int id;
    private String uuid;
    private transient TreeSet<String> oldTags;
    private transient JsonObject oldNotes = EMPTY_JSON_OBJ;
    private TreeSet<String> tags;
    private JsonObject notes = EMPTY_JSON_OBJ;

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getUuid() {
        return uuid;
    }
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
    public TreeSet<String> getTags() {
        return tags;
    }
    public void setTags(TreeSet<String> tags) {
        this.tags = tags;
    }
    public JsonObject getNotes() {
        return notes;
    }
    public void setNotes(JsonObject notes) {
        this.notes = notes;
    }
    
    public TreeSet<String> getOldTags() {
        return oldTags;
    }
    public void setOldTags(TreeSet<String> oldTags) {
        this.oldTags = oldTags;
    }
    public JsonObject getOldNotes() {
        return oldNotes;
    }
    public void setOldNotes(JsonObject oldNotes) {
        this.oldNotes = oldNotes;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result + ((notes == null) ? 0 : notes.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JobAnnotation other = (JobAnnotation) obj;
        if (id != other.id)
            return false;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        if (tags == null) {
            if (other.tags != null)
                return false;
        } else if (!tags.equals(other.tags))
            return false;
        if (notes == null) {
            if (other.notes != null)
                return false;
        } else if (!notes.equals(other.notes))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return "JobAnnotation [id=" + id + ", uuid=" + uuid + ", tags=" + tags + ", notes=" + notes + "]";
    }
}
