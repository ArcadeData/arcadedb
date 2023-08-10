package com.arcadedb.server.security.oidc;
public class Configuration
{
    private final String clientId = "df-backend";
    private final String secret = "Uubtps4JcmkUoo9BXQySDmSW";
    private final String discoveryURI = "http://localhost:8180/auth/realms/data-fabric/.well-known/openid-configuration";
    private final String responseType = "code";
    private final String scope = "openid profile";
    public String getClientId()
    {
        return clientId;
    }
    public String getSecret()
    {
        return secret;
    }
    public String getDiscoveryURI()
    {
        return discoveryURI;
    }
    public String getResponseType()
    {
        return responseType;
    }
    public String getScope()
    {
        return scope;
    }
}