package com.arcadedb.security;

import java.util.Map;
import java.util.Set;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;

public class AuthorizationUtils {

  /**
   * The valid classification options, in order of increasing sensitivity. Permits math comparisons.
   * There may be a faster/better way of doing this, but bigger fish to fry first
   */
  public static final Map<String, Integer> classificationOptions = Map.of("U", 0, "CUI", 1, "C", 2, "S", 3, "TS", 4);

  /**
   * Checks if the provided classification is permitted given the deployment classification.
   * @param classification
   * @return
   */
  public static boolean isClassificationValidForDeployment(final String classification) {
    if (classification == null || classification.isEmpty())
      return false;

    var c = classification.toUpperCase();

    if (c.contains("//")) {
        c = getClassificationFromResourceMarkings(c);
    }

    if (!classificationOptions.containsKey(c))
      throw new IllegalArgumentException("Classification must be one of " + classificationOptions);

    var deploymentClassification = System.getProperty("deploymentClassifcation", "S");
    return classificationOptions.get(deploymentClassification) >= classificationOptions.get(c);
  }

  /**
   * Checks if the user is authorized to view the resource, taking into account the user's clearance, attributes, the full resouce ACCM markings.
   * @param userClearance
   * @param nationality
   * @param resourceClassification
   * @return
   */
  public static boolean isUserAuthorizedForResourceMarking(final String userClearance, final String nationality, final String tetragraphs,
        final String resourceClassification) {
    
    // TODO are tetrapgrahs that group countries auto applicable to users of those countires, or do users need explicit authoirzation for their data?
    var processedResourceClassification = resourceClassification.replace("FVEY", "USA,AUS,CAN,GBR,NZL");
    if (!isClearanceAuthorized(userClearance, processedResourceClassification)) {
      // System.out.println("blocked by clearance");
      return false;
    }

    // NO FORN takes precedence over REL TO?
    if (isBlockedByNoForn(nationality, processedResourceClassification)) {
      // System.out.println("blocked by noforn");
      return false;
    }

    if (isBlockedByReleaseableTo(nationality, tetragraphs, processedResourceClassification)) {
      // System.out.println("blocked by noforn");
      return false;
    }

    return true;
  }

  /**
   * Checks if the user has sufficient clearance to view the resource, outside of any other ACCM restrictions.
   * @param userClearance
   * @param resourceClassificationMarking
   * @return
   */
  public static boolean isClearanceAuthorized(final String userClearance, final String resourceClassificationMarking) {
    if (userClearance == null || userClearance.isEmpty())
      return false;

    String processedUserClearance = userClearance.toUpperCase();
    processedUserClearance = processedUserClearance.trim();

    String processedResourceClearance = getClassificationFromResourceMarkings(resourceClassificationMarking);
    processedResourceClearance = processedResourceClearance.trim();

    if (!classificationOptions.containsKey(processedUserClearance))
      throw new IllegalArgumentException("Clearance must be one of " + classificationOptions);

    if (resourceClassificationMarking == null || resourceClassificationMarking.isEmpty())
      return false;

    if (!classificationOptions.containsKey(processedResourceClearance))
      throw new IllegalArgumentException("Invalid resource classification " + processedResourceClearance);

    return classificationOptions.get(processedUserClearance) >= classificationOptions.get(processedResourceClearance);
  }

  /**
   * Checks if the classification markings contains a releaseable to block, and if so, checks if the user
   * belongs to an allowable nationality.
   * @param nationality
   * @param resouceClassificationMarkings
   * @return
   */
  private static boolean isBlockedByReleaseableTo(final String nationality, final String tetragraphs, 
        final String resourceClassificationMarkings) {
    // TODO add support for banner barking AUTHORIZED FOR RELEASE TO
    if (resourceClassificationMarkings.contains("REL TO")) {
        if (nationality == null || nationality.isEmpty()) {
          return true;
        }

        var releaseableTo = resourceClassificationMarkings.substring(resourceClassificationMarkings.indexOf("REL TO"));
        if (releaseableTo.contains("//")) {
            releaseableTo.substring(0, releaseableTo.indexOf("//"));
        }

        releaseableTo = releaseableTo.substring("REL TO".length() + 1);
        releaseableTo = releaseableTo.replaceAll(" ", "");

        if (releaseableTo.contains(",")) {
            return !Set.of(releaseableTo.split(",")).stream().map(r -> r.toString()).anyMatch(r -> {
              if (r.trim().isEmpty()) {
                return false;
              } else if (r.length() == 3) {
                return r.equals(nationality);
              } else if (tetragraphs != null && !tetragraphs.isEmpty() && r.length() == 4) {
                return tetragraphs.contains(r);
              }
              return false;
            });
        } else {
            return !releaseableTo.equals(nationality);
        }
    }

    return false;
  }

  /**
   * Checks if the user is unable to see the resource due to a NOFORN marking.
   * @param nationality
   * @param resourceClassification
   * @return
   */
  private static boolean isBlockedByNoForn(final String nationality, final String resourceClassification) {
    return (containsBlockText("NOFORN", resourceClassification) || containsBlockText("NF", resourceClassification)) 
        && (nationality == null || !nationality.equals("USA"));
  }

  /**
   * Checks if the resource classification markings contains the text between single forward slash blocks.
   * @param text
   * @param resourceClassification
   * @return
   */
  private static boolean containsBlockText(final String text, final String resourceClassification) {
    if (resourceClassification.contains("/")) {
      return Set.of(resourceClassification.split("/")).contains(text);
    } else if (resourceClassification.equals(text)) {
      return true;
    } else {
      // if the string ends with the text, or starts with the text with a space after it, or contains the text with a space before and after it
      return resourceClassification.endsWith(" " + text) || resourceClassification.endsWith("-" + text) 
          || resourceClassification.startsWith(text + " ") || resourceClassification.contains(" " + text + " ");
    }
  }

  /**
   * Remove the parenthesis and any spaces from the classification string that may be a part of a portion marking.
   * @param resourceClassification
   * @return
   */
  private static String cleanResourceClassification(String resourceClassification) {
    resourceClassification = resourceClassification.toUpperCase();
    resourceClassification = resourceClassification.replace("(", "");
    resourceClassification = resourceClassification.replace(")", "");
    resourceClassification = resourceClassification.trim();
    return resourceClassification;
  }

  /**
   * Parse out just the classification from the full classification attribution string.
   * @param resourceMarkings
   * @return
   */
  private static String getClassificationFromResourceMarkings(final String resourceMarkings) {

    if (resourceMarkings == null || resourceMarkings.isEmpty()) {
      return null;
    }

    String classification = cleanResourceClassification(resourceMarkings);

    if (classification.contains("//")) {
        return classification.substring(0, classification.indexOf("//"));
    }
    return classification;
  }

  public static boolean checkPermissionsOnDocumentToRead(final Document document, final SecurityDatabaseUser currentUser) {
    return checkPermissionsOnDocument(document, currentUser, false);
  }

  public static boolean checkPermissionsOnDocumentToWrite(final Document document, final SecurityDatabaseUser currentUser) {
    return checkPermissionsOnDocument(document, currentUser, true);
  }

  private static boolean checkPermissionsOnDocument(final Document document, final SecurityDatabaseUser currentUser, final boolean isWriteAction) {
    // Allow root user to access all documents for HA syncing between nodes
    if (currentUser.getName().equals("root")) {
      return true;
    }

    // TODO short term - check classification, attribution on document

    // TODO long term - replace with filtering by low classification of related/linked document.
    // Expensive to do at read time. Include linkages and classification at write time?
    // Needs performance testing and COA analysis.

    if (currentUser.isServiceAccount() || currentUser.isDataSteward(document.getTypeName())) {
      return true;
    }

    // Prevent users from accessing documents that have not been marked, unless we're evaluating a user's permission to a doc that hasn't been created yet.
    if (!isWriteAction && (!document.has(MutableDocument.CLASSIFICATION_MARKED) || !document.getBoolean(MutableDocument.CLASSIFICATION_MARKED))) {
      // todo throw illegal arg exception, no valid marking
      return false;
    }

    // TODO detect and provide org for clearance
    var clearance = currentUser.getClearanceForCountryOrTetragraphCode("USA");
    var nationality = currentUser.getNationality();
    var tetragraphs = currentUser.getTetragraphs();

    if (document.has(MutableDocument.SOURCES)) {
      // sources will be a map, in the form of source number : (classification//ACCM) source id
      // check if user has appropriate clearance for any of the sources for the document
      var isSourceAuthorized = document.toJSON().getJSONObject(MutableDocument.SOURCES).toMap().entrySet().stream().anyMatch(s -> {
        
        var source = s.getValue().toString();
        if (source == null || source.isEmpty()) {
          return false;
        }

        // if the source is not in the form of (classification/[/ACCM[]) [source id], then it is not a valid source
        if (!source.contains("(") || !source.contains(")")) {
          return false;
        }

        var sourceClassification = source.substring(source.indexOf("(") + 1, source.indexOf(")"));
        return AuthorizationUtils.isUserAuthorizedForResourceMarking(clearance, nationality, tetragraphs, sourceClassification);
      });

      if (isSourceAuthorized) {
        return true;
      }
    }

    if (document.has(MutableDocument.CLASSIFICATION_PROPERTY) 
          && document.toJSON().getJSONObject(MutableDocument.CLASSIFICATION_PROPERTY).has(MutableDocument.CLASSIFICATION_GENERAL_PROPERTY)) {
      var docClassification = 
          document.toJSON().getJSONObject(MutableDocument.CLASSIFICATION_PROPERTY).getString(MutableDocument.CLASSIFICATION_GENERAL_PROPERTY);
      if (docClassification != null && !docClassification.isEmpty()) {
        var isAuthorized = isUserAuthorizedForResourceMarking(clearance, nationality, tetragraphs, docClassification);
        return isAuthorized;
      } else {
        return false;
      }
    }

    return false;
  }
}
