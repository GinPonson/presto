package com.facebook.presto.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author Gin
 * @since 2018/8/17.
 */
public class CatalogInfo
{

    private final String catalogName;

    private final String connectorName;

    private final Map<String, String> properties;

    @JsonCreator
    public CatalogInfo(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("properties")Map<String, String> properties)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getConnectorName() {
        return connectorName;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }
}
