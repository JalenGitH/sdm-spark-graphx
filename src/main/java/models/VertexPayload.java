package models;

import java.io.Serializable;
import java.util.ArrayList;

public class VertexPayload implements Serializable {
    public Integer value;
    public ArrayList<Long> path;

    public VertexPayload(Integer value, ArrayList<Long> path) {
        this.value = value;
        this.path = path;
    }
}
