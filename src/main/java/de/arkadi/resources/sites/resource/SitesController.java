
package de.arkadi.resources.sites.resource;

import com.google.common.base.Strings;
import de.arkadi.common.exception.RestException;
import de.arkadi.common.response.PaginatedResponse;
import de.arkadi.resources.sites.model.Site;
import de.arkadi.resources.sites.model.SitesDataMapper;
import de.arkadi.resources.sites.model.SitesRequest;

import java.util.ArrayList;
import java.util.List;

public class SitesController {
    private final SitesDataMapper SITES_DATA_MAPPER;
    
    private SitesController() {
        SITES_DATA_MAPPER = new SitesDataMapper();
    }

    private static class SitesControllerHolder {
        public static final SitesController instance = new SitesController();
    }

    /**
     * Get a thread-safe singleton instance of this
     * 
     * @return 
     */
    public static SitesController getInstance() {
        return SitesControllerHolder.instance;
    }
    
    /**
     * Get top sites by request
     * 
     * @param sitesRequest - initalized and verified by caller
     * @return
     * @throws RestException 
     */
    public PaginatedResponse getTopSites(SitesRequest sitesRequest) throws RestException {
        int total;
        List<Site> items;
        
        if(!Strings.isNullOrEmpty(sitesRequest.getTld())){
            total = SITES_DATA_MAPPER.getTldListTotal(sitesRequest.getTld());
            items = SITES_DATA_MAPPER.getTldSiteList(sitesRequest.getTld(), sitesRequest.getPage(), sitesRequest.getSize());
        }else{
            total = SITES_DATA_MAPPER.getSitesRankListTotal();
            items = SITES_DATA_MAPPER.getRankedSitesList(sitesRequest.getPage(), sitesRequest.getSize());
        }
        
        return buildPaginatedResponse(sitesRequest, total, items);
    }
    
    /**
     * Build paginated response
     * 
     * Note: this handles overflowing requests, and sets previous to last page with entries
     * 
     * @param sitesRequest
     * @param total
     * @param items
     * @return 
     */
    protected PaginatedResponse buildPaginatedResponse(SitesRequest sitesRequest, int total, List<Site> items){
        Integer prev = null;
        Integer next = null;
        if(sitesRequest.getPage() > 1){
            if(total > sitesRequest.getPage() * sitesRequest.getSize()){
                prev = sitesRequest.getPage() -1;
            }else{
                prev = total / sitesRequest.getSize();
            }
        }
        
        if(total > sitesRequest.getPage() * sitesRequest.getSize()){
            next = sitesRequest.getPage() + 1;
        }
        
        return new PaginatedResponse(total, new ArrayList<>(items), next, prev);
    }
    
    
}
