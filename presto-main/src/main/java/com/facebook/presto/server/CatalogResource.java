/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;

/**
 * @author Gin
 * @since 2018/8/15.
 */
@Path("/v1/catalog")
public class CatalogResource
{
    private final ConnectorManager connectorManager;
    private final Announcer announcer;

    @Inject
    public CatalogResource(
            ConnectorManager connectorManager,
            Announcer announcer)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
    }

    @GET
    @Path("test")
    public Response test()
    {
        return Response.ok("Hello world").build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createOrUpdateTask(CatalogInfo catalogInfo)
    {
        requireNonNull(catalogInfo, "catalogInfo is null");

        ConnectorId connectorId = connectorManager.createConnection(
                catalogInfo.getCatalogName(),
                catalogInfo.getConnectorName(),
                catalogInfo.getProperties());

        updateConnectorIdAnnouncement(announcer, connectorId);
        return Response.status(Response.Status.OK).build();
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, ConnectorId connectorId)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }
}
