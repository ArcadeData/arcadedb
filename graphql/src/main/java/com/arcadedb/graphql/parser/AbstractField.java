package com.arcadedb.graphql.parser;

public abstract class AbstractField extends SimpleNode {

  protected Name       name;
  protected Directives directives;

  public AbstractField(int id) {
    super(id);
  }

  public AbstractField(GraphQLParser p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(GraphQLParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public String getName() {
    return name != null ? name.value : null;
  }

  public Directives getDirectives() {
    return directives;
  }
}
