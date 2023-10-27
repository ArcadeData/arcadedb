package com.arcadedb.security.ACCM;

import java.util.ArrayList;
import java.util.List;

import com.arcadedb.schema.Type;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@Accessors(chain = true, fluent = true)
public class AccmProperty {

    // Split name by periods into property path
    private String name;
    private String parentType;
    private Type dataType;
    private boolean required;
    private boolean notNull;
    private boolean readOnly;
    private List<String> options = new ArrayList<>();
    private String validationRegex;

   // private String keycloakUserInfoPropertyName;
   // might also be coming from roles
}
