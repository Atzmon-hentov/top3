
package io.kafkastreams.monitoring.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "ts",
    "InventoryID"
})
public class AdClick {

    @JsonProperty("ts")
    private Long ts;

    @JsonProperty("ts")
    public Long getTs() {
        return ts;
    }

    @JsonProperty("InventoryID")
    private String inventoryID;

    @JsonProperty("InventoryID")
    public String getInventoryID() {
        return inventoryID;
    }

    @JsonProperty("InventoryID")
    public void setInventoryID(String inventoryID) {
        this.inventoryID = inventoryID;
    }

    public AdClick withInventoryID(String inventoryID) {
        this.inventoryID = inventoryID;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("inventoryID", inventoryID).toString();
    }

}
