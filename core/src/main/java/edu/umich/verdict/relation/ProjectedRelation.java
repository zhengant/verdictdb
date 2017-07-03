package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;

public class ProjectedRelation extends ExactRelation {
	
	private ExactRelation source; 
	
	private List<SelectElem> elems;

	public ProjectedRelation(VerdictContext vc, ExactRelation source, List<SelectElem> elems) {
		super(vc);
		this.source = source;
		this.elems = elems;
	}
	
	public ExactRelation getSource() {
		return source;
	}

	@Override
	protected String getSourceName() {
		return getAliasName();
	}

	@Override
	public ApproxRelation approx() throws VerdictException {
		ApproxRelation a = new ApproxProjectedRelation(vc, source.approx(), elems);
		a.setAliasName(getAliasName());
		return a;
	}

	@Override
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return null;
	}
	
	protected String selectSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		List<String> elemWithAlias = new ArrayList<String>();
		for (SelectElem e : elems) {
			if (e.getAlias() != null) {
				elemWithAlias.add(String.format("%s AS %s", e.getExpr(), e.getAlias()));
			} else {
				elemWithAlias.add(e.getExpr().toString());
			}
		}
		sql.append(Joiner.on(", ").join(elemWithAlias));
		return sql.toString();
	}

	@Override
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append(selectSql());
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(this.source);
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";
		
		sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		
		return sql.toString();
	}

	@Override
	public List<SelectElem> getSelectList() {
		return elems;
	}

}
