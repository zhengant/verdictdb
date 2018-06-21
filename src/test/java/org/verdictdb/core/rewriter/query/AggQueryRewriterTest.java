package org.verdictdb.core.rewriter.query;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.rewriter.AliasRenamingRules;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.scramble.Scrambler;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.HiveSyntax;

public class AggQueryRewriterTest {

  int aggblockCount = 10;

  ScrambleMeta generateTestScrambleMeta() {
    ScrambleMeta meta = new ScrambleMeta();
    meta.insertScrambleMetaEntry("myschema", "mytable",
        Scrambler.getAggregationBlockColumn(),
        Scrambler.getSubsampleColumn(),
        Scrambler.getTierColumn(),
        aggblockCount);
    return meta;
  }
  
  String quoteAlias(String alias) {
    return "`" + alias + "`";
  }

  @Test
  public void testSelectSumBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "sum(verdictdbalias4.`verdictdbalias6`) as " + quoteAlias(aliasForSumEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias6` * "
          + "verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumScaledSubsum) + ", "
          + "sum((verdictdbalias4.`verdictdbalias6` * verdictdbalias4.`verdictdbalias6`) * "
          + "verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumSquaredScaledSubsum) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from ("
          + "select verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "sum(verdictdbalias1.`mycolumn1`) as `verdictdbalias6`, "
          + "sum(case when verdictdbalias1.`mycolumn1` is not null then 1 else 0 end) as `verdictdbalias7` "
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`) as verdictdbalias4 "
          + "group by `verdictdb:tier`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectCountBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("count"), aliasName)), base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();
    String aliasForTier = AliasRenamingRules.tierAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "sum(verdictdbalias4.`verdictdbalias6`) as " + quoteAlias(aliasForCountEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias6` * "
          + "verdictdbalias4.`verdictdbalias6`) as " + quoteAlias(aliasForSumScaledSubcount) + ", "
          + "sum(pow(verdictdbalias4.`verdictdbalias6`, 3)) as " + quoteAlias(aliasForSumSquaredScaledSubcount) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias6`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from ("
          + "select verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "count(*) as `verdictdbalias6` "
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`) as verdictdbalias4 "
          + "group by `verdictdb:tier`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectAvgBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "sum(verdictdbalias4.`verdictdbalias6`) as " + quoteAlias(aliasForSumEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForCountEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias6` * "
          + "verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumScaledSubsum) + ", "
          + "sum((verdictdbalias4.`verdictdbalias6` * verdictdbalias4.`verdictdbalias6`) * "
          + "verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumSquaredScaledSubsum) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7` * "
          + "verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumScaledSubcount) + ", "
          + "sum(pow(verdictdbalias4.`verdictdbalias7`, 3)) as " + quoteAlias(aliasForSumSquaredScaledSubcount) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from (select "
          + "verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "sum(verdictdbalias1.`mycolumn1`) as `verdictdbalias6`, "    // subsum
          + "sum(case when verdictdbalias1.`mycolumn1` is not null then 1 else 0 end) as `verdictdbalias7` "    // subsample size
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`) as verdictdbalias4 "
          + "group by `verdictdb:tier`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectSumGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "mygroup"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    relation.addGroupby(new BaseColumn("t", "mygroup"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "verdictdbalias4.`verdictdbalias6` as `mygroup`, "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7` * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumScaledSubsum) + ", "
          + "sum((verdictdbalias4.`verdictdbalias7` * verdictdbalias4.`verdictdbalias7`) * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumSquaredScaledSubsum) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from ("
          + "select verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "verdictdbalias1.`mygroup` as `verdictdbalias6`, "
          + "sum(verdictdbalias1.`mycolumn1`) as `verdictdbalias7`, "
          + "sum(case when verdictdbalias1.`mycolumn1` is not null then 1 else 0 end) as `verdictdbalias8` "
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`, `verdictdbalias6`) as verdictdbalias4 "
          + "group by `verdictdb:tier`, `mygroup`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectSumGroupby2BaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "myalias"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    relation.addGroupby(new AliasReference("myalias"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "verdictdbalias4.`verdictdbalias6` as `myalias`, "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7` * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumScaledSubsum) + ", "
          + "sum((verdictdbalias4.`verdictdbalias7` * verdictdbalias4.`verdictdbalias7`) * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumSquaredScaledSubsum) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from ("
          + "select verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "verdictdbalias1.`mygroup` as `verdictdbalias6`, "
          + "sum(verdictdbalias1.`mycolumn1`) as `verdictdbalias7`, "
          + "sum(case when verdictdbalias1.`mycolumn1` is not null then 1 else 0 end) as `verdictdbalias8` "
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`, `verdictdbalias6`) as verdictdbalias4 "
          + "group by `verdictdb:tier`, `myalias`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectCountGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "mygroup"),
            new AliasedColumn(new ColumnOp("count"), aliasName)), base);
    relation.addGroupby(new BaseColumn("t", "mygroup"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "verdictdbalias4.`verdictdbalias6` as `mygroup`, "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForCountEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7` * "
          + "verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumScaledSubcount) + ", "
          + "sum(pow(verdictdbalias4.`verdictdbalias7`, 3)) as " + quoteAlias(aliasForSumSquaredScaledSubcount) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from ("
          + "select verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "verdictdbalias1.`mygroup` as `verdictdbalias6`, "
          + "count(*) as `verdictdbalias7` "
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`, `verdictdbalias6`) as verdictdbalias4 "
          + "group by `verdictdb:tier`, `mygroup`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectAvgGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    relation.addGroupby(new BaseColumn("t", "mygroup"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias4.`verdictdbalias5` as `verdictdb:tier`, "
          + "verdictdbalias4.`verdictdbalias6` as `mygroup`, "
          + "sum(verdictdbalias4.`verdictdbalias7`) as " + quoteAlias(aliasForSumEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForCountEstimate) + ", "
          + "sum(verdictdbalias4.`verdictdbalias7` * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumScaledSubsum) + ", "
          + "sum((verdictdbalias4.`verdictdbalias7` * verdictdbalias4.`verdictdbalias7`) * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumSquaredScaledSubsum) + ", "
          + "sum(verdictdbalias4.`verdictdbalias8` * "
          + "verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumScaledSubcount) + ", "
          + "sum(pow(verdictdbalias4.`verdictdbalias8`, 3)) as " + quoteAlias(aliasForSumSquaredScaledSubcount) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias4.`verdictdbalias8`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from (select "
          + "verdictdbalias1.`verdictdbalias3` as `verdictdbalias5`, "
          + "verdictdbalias1.`mygroup` as `verdictdbalias6`, "
          + "sum(verdictdbalias1.`mycolumn1`) as `verdictdbalias7`, "    // subsum
          + "sum(case when verdictdbalias1.`mycolumn1` is not null then 1 else 0 end) as `verdictdbalias8` "    // subsample size
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1 "
          + "group by verdictdbalias1.`verdictdbalias2`, `verdictdbalias5`, `verdictdbalias6`) as verdictdbalias4 "
          + "group by `verdictdb:tier`, `mygroup`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectSumNestedTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp nestedSource = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(
                ColumnOp.multiply(new BaseColumn("t", "price"), new BaseColumn("t", "discount")),
                "discounted_price")),
        base);
    nestedSource.setAliasName("s");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("s", "discounted_price")), aliasName)),
        nestedSource);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select verdictdbalias6.`verdictdbalias7` as `verdictdb:tier`, "
          + "sum(verdictdbalias6.`verdictdbalias8`) as " + quoteAlias(aliasForSumEstimate) + ", "
          + "sum(verdictdbalias6.`verdictdbalias8` * "
          + "verdictdbalias6.`verdictdbalias9`) as " + quoteAlias(aliasForSumScaledSubsum) + ", "
          + "sum((verdictdbalias6.`verdictdbalias8` * verdictdbalias6.`verdictdbalias8`) * "
          + "verdictdbalias6.`verdictdbalias9`) as " + quoteAlias(aliasForSumSquaredScaledSubsum) + ", "
          + "count(*) as " + quoteAlias(aliasForCountSubsample) + ", "
          + "sum(verdictdbalias6.`verdictdbalias9`) as " + quoteAlias(aliasForSumSubsampleSize) + " "
          + "from ("
          + "select s.`verdictdbalias5` as `verdictdbalias7`, "
          + "sum(s.`discounted_price`) as `verdictdbalias8`, "
          + "sum(case when s.`discounted_price` is not null then 1 else 0 end) as `verdictdbalias9` "
          + "from (select verdictdbalias1.`price` * verdictdbalias1.`discount` as `discounted_price`, "
          + "verdictdbalias1.`verdictdbalias2` as `verdictdbalias4`, "
          + "verdictdbalias1.`verdictdbalias3` as `verdictdbalias5` "
          + "from (select *, "
          + "t.`verdictdbsid` as `verdictdbalias2`, "
          + "t.`verdictdbtier` as `verdictdbalias3` "
          + "from `myschema`.`mytable` as t "
          + "where t.`verdictdbaggblock` = " + k + ") as verdictdbalias1) as s "
          + "group by s.`verdictdbalias4`, `verdictdbalias7`) as verdictdbalias6 "
          + "group by `verdictdb:tier`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }
}