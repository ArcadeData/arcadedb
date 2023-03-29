CREATE DOCUMENT TYPE doc;
CREATE PROPERTY doc.uri1 STRING (regexp '^([a-zA-Z]{1,15}:)(\\/\\/)?[^\\s\\/$.?#].[^\\s]*$');
