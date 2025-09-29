package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.List;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class ReqJobAnnotation implements IReqBody
{
    // Tags and notes to be added to the job annotation.
    private List<String>            tags;   
    private JsonObject                  notes;

    // setters and getters
    public List<String> getTags() {return tags;}
    public void setTags(List<String> tags) {this.tags = tags;}
    public JsonObject getNotes() {return notes;}
    public void setNotes(JsonObject notes) {this.notes = notes;}

    @Override
    public String validate() {
        // list of tags should only contain less than 128 tags. 
        if (tags != null && tags.size() > 128) {
            return MsgUtils.getMsg("SK_INVALID_PARAMETER", "tags", "must contain less than 128 items");
        }
        // TODO: currently there is no need to add validation for notes object. 
        return null;
    }
    
}
