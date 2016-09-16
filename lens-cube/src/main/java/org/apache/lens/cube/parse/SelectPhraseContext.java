package org.apache.lens.cube.parse;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.Data;

@Data
class SelectPhraseContext extends QueriedPhraseContext {
  private String actualAlias;
  private String selectAlias;
  private String finalAlias;
  private String exprWithoutAlias;

  public SelectPhraseContext(ASTNode selectExpr) {
    super(selectExpr);
  }

  String getExprWithoutAlias() {
    if (exprWithoutAlias == null) {
      //Order of Children of select expression AST Node => Index 0: Select Expression Without Alias, Index 1: Alias */
      exprWithoutAlias = HQLParser.getString((ASTNode) getExprAST().getChild(0)).trim();
    }
    return exprWithoutAlias;
  }

  void updateExprs() {
    super.updateExprs();
    exprWithoutAlias = HQLParser.getString((ASTNode) getExprAST().getChild(0)).trim();
  }

}
