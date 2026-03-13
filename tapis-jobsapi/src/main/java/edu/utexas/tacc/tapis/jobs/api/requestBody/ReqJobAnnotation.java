package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.List;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/**
 * Request body for patch/putJobAnnotations (tags and notes) to a job.
 * 
 * @author wei.zhang@tacc.utexas.edu
 */
public class ReqJobAnnotation implements IReqBody
{

    // ============================================================
    //                          Fields
    // ============================================================

    // Tags and notes to be added to the job annotation.
    private List<String>            tags;   
    private JsonObject              notes;

    // ============================================================
    //                    Getters and Setters
    // ============================================================
    public List<String> getTags() {return tags;}
    public void setTags(List<String> tags) {this.tags = tags;}
    public JsonObject getNotes() {return notes;}
    public void setNotes(JsonObject notes) {this.notes = notes;}

    // ============================================================
    //                      Public Methods
    // ============================================================
    @Override
    public String validate() {
        return null;
    }
}
