package com.arcadedb.server.security.oidc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Helper class to support serialization/deserialization of arcade ACL config
 */
@Data
@NoArgsConstructor
public class Group {
    @JsonIgnore
    private String name; // name is not in object, it is the key of the object

    @JsonIgnore
    private String database = "none";

    private List<String> access = new ArrayList<>();

    private int readTimeout = -1;
    private int resultSetLimit = -1;

    /** Map of database type names and corresponding access to the types */
    private Map<String, GroupTypeAccess> types = new HashMap<>();

    public String getArcadeName() {
        return name != null ? name
                : String.format("database: %s; access: %s; types %s; readTimeout: %n; resultSetLimit: %n",
                        database, access.toString(), types.toString(), readTimeout, resultSetLimit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Group other = (Group) obj;
        if (database == null) {
            if (other.database != null)
                return false;
        } else if (!database.equals(other.database))
            return false;
        if (access == null) {
            if (other.access != null)
                return false;
        } else if (!access.equals(other.access))
            return false;
        if (readTimeout != other.readTimeout)
            return false;
        if (resultSetLimit != other.resultSetLimit)
            return false;
        if (types == null) {
            if (other.types != null)
                return false;
        } else if (!types.equals(other.types))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((access == null) ? 0 : access.hashCode());
        result = prime * result + readTimeout;
        result = prime * result + resultSetLimit;
        result = prime * result + ((types == null) ? 0 : types.hashCode());
        return result;
    }
}
