package de.arkadi.server;

import de.arkadi.common.filter.CorsFilter;
import de.arkadi.common.filter.RestExceptionMapper;
import de.arkadi.resources.sites.resource.SitesResource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import org.jboss.resteasy.plugins.cache.server.ServerCacheFeature;


@ApplicationPath("")
public class RestApplication extends Application{

    @Override
    public Set<Class<?>> getClasses() {
        return Collections.emptySet();
    }

    @Override
    public Set<Object> getSingletons() {
        Set<Object> singletons = new HashSet<>();

        singletons.add(new CorsFilter());
        singletons.add(new RestExceptionMapper());
        singletons.add(new ServerCacheFeature());
        singletons.add(new SitesResource());

        return singletons;
    }
}