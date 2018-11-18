
package de.arkadi.cli;

import com.google.common.base.Strings;
import java.util.Map;

public class Environment {
    private static final Map<String, String> ENV = System.getenv();
    
    /**
     * Get all environment variables
     * 
     * @return 
     */
    public static Map<String, String> getAll(){
        return ENV;
    }
    
    /**
     * Get an environment variable
     * 
     * @param key the variable name
     * @return 
     */
    public static String get(String key){
        return ENV.get(key);
    }
    
    /**
     * Get a string environment variable
     * 
     * @param key
     * @param defaultValue
     * @return 
     */
    public static String getOrDefault(String key, String defaultValue){
        return ENV.getOrDefault(key, defaultValue);
    }
    
    /**
     * Get an integer environment variable
     * 
     * @param key the variable name
     * @param defaultValue used if can't be formatted or not found
     * @return 
     */
    public static Integer getOrDefault(String key, int defaultValue){
        try{
            String value = get(key);
            if(Strings.isNullOrEmpty(value)){
                return defaultValue;
            }
            
            return Integer.parseInt(value);
            
        }catch(NumberFormatException ex){
            
        }
        
        return defaultValue;
    }
}
