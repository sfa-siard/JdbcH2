/*======================================================================
H2DatabaseMetaData implements wrapped H2 DatabaseMetaData.
Version     : $Id: $
Application : SIARD2
Description : H2DatabaseMetaData implements wrapped H2 DatabaseMetaData.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 10.05.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.util.*;
import org.h2.jdbc.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.sqlparser.SqlLiterals;

/*====================================================================*/
/** H2DatabaseMetaData implements wrapped H2 DatabaseMetaData.
 * @author Hartwig Thomas
 */
public class H2DatabaseMetaData
  extends BaseDatabaseMetaData
  implements DatabaseMetaData
{
  /*------------------------------------------------------------------*/
  /** convert an H2 JdbcSQLException into an SQLException.
   * @param jse
   * @throws SQLException
   */
  private void throwSqlException(JdbcSQLException jse)
    throws SQLException
  {
    throw new SQLException("H2 exception!", jse);
  } /* throwSqlException */
  
  /*------------------------------------------------------------------*/
  /** convert an H2 JdbcSQLException into an SQLFeatureNotSupportedException.
   * @param jse
   * @throws SQLFeatureNotSupportedException
   */
  private void throwNotSupportedException(JdbcSQLException jse)
    throws SQLFeatureNotSupportedException
  {
    throw new SQLFeatureNotSupportedException("H2 Exception!", jse);
  } /* throwFeatureNotSupportedSqlException */
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param dmdWrapped database meta data to be wrapped.
   */
  public H2DatabaseMetaData(DatabaseMetaData dmdWrapped)
  {
    super(dmdWrapped);
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Connection getConnection() throws SQLException
  {
    Connection conn = null;
    try { conn = new H2Connection(super.getConnection()); }
    catch(JdbcSQLException jse) { throwSqlException(jse); }
    return conn;
  } /* getConnection */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public int getMaxBinaryLiteralLength() throws SQLException
  {
    return Integer.MAX_VALUE;
  } /* getMaxBinaryLiteralLength */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public int getMaxCharLiteralLength() throws SQLException
  {
    return Integer.MAX_VALUE;
  } /* getMaxCharLiteralLength */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Use H2MetaColumn for data type translation.
   */
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
    String tableNamePattern, String columnNamePattern)
    throws SQLException
  {
    return new H2MetaColumns(
      super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern),
      getConnection(), 1,2,3,4,5,6,7,7,9);
  } /* getColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getProcedureColumns(String catalog,
    String schemaPattern, String procedureNamePattern,
    String columnNamePattern) throws SQLException
  {
    return new H2MetaColumns(
      super.getProcedureColumns(catalog, schemaPattern,procedureNamePattern, columnNamePattern),
      getConnection(),6,7,8,9,10);
  } /* getProcedureColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} 
   * Convert JdbcSQLException from H2 into SQLFeatureNotSupportedError.
   */
  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern,
    String typeNamePattern) throws SQLException
  {
    ResultSet rs = null;
    try { rs = super.getSuperTypes(catalog, schemaPattern, typeNamePattern); }
    catch(JdbcSQLException jse) { throwNotSupportedException(jse); }
    return rs;
  } /* getSuperTypes */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Convert JdbcSQLException from H2 into SQLFeatureNotSupportedError.
   */
  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
    String typeNamePattern, String attributeNamePattern)
    throws SQLException
  {
    ResultSet rs = null;
    try { rs = super.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern); }
    catch(JdbcSQLException jse) { throwNotSupportedException(jse); }
    return rs;
  } /* getAttributes */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Convert JdbcSQLException from H2 into SQLFeatureNotSupportedError.
   */
  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
    String functionNamePattern) throws SQLException
  {
    ResultSet rs = null;
    try { rs = super.getFunctions(catalog, schemaPattern, functionNamePattern); }
    catch(JdbcSQLException jse) { throwNotSupportedException(jse); }
    return rs;
  } /* getFunctions */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getFunctionColumns(String catalog,
    String schemaPattern, String functionNamePattern,
    String columnNamePattern) throws SQLException
  {
    ResultSet rs = null;
    try { rs = super.getFunctionColumns(catalog, schemaPattern, 
      functionNamePattern, columnNamePattern); }
    catch(JdbcSQLException jse) { throwNotSupportedException(jse); }
    return rs;
  } /* getFunctionColumns */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
    throws SQLException
  {
    ResultSet rs = null;
    /*** replace faulty (from H2):    
    .prepareAutoCloseStatement("SELECT "
      + "SCHEMA_NAME TABLE_SCHEM, "
      + "CATALOG_NAME TABLE_CATALOG, "
      +" IS_DEFAULT "
      + "FROM INFORMATION_SCHEMA.SCHEMATA "
      + "WHERE CATALOG_NAME LIKE ? ESCAPE ? "
      + "AND SCHEMA_NAME LIKE ? ESCAPE ? "
      + "ORDER BY SCHEMA_NAME");
    prep.setString(1, getCatalogPattern(catalogPattern));
    prep.setString(2, "\\");
    prep.setString(3, getSchemaPattern(schemaPattern));
    prep.setString(4, "\\");
    ***/
    StringBuilder sbSql = new StringBuilder("SELECT\r\n" +
      "  CATALOG_NAME AS TABLE_CATALOG,\r\n"+
      "  SCHEMA_NAME AS TABLE_SCHEM\r\n" +
      "FROM INFORMATION_SCHEMA.SCHEMATA");
    if (catalog != null)
    {
      sbSql.append("\r\nWHERE CATALOG_NAME = "+SqlLiterals.formatStringLiteral(catalog));
      if (schemaPattern != null)
        sbSql.append("\r\n  AND");
    }
    else if (schemaPattern != null)
      sbSql.append("\r\nWHERE");
    if (schemaPattern != null)
      sbSql.append(" SCHEMA_NAME LIKE "+
        SqlLiterals.formatStringLiteral(schemaPattern) +
        " ESCAPE " + 
        SqlLiterals.formatStringLiteral(super.getSearchStringEscape()));
    Statement stmt = getConnection().createStatement();
    rs = stmt.unwrap(Statement.class).executeQuery(sbSql.toString());
    return rs;
  } /* getSchemas */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} 
   * is corrected to return the same name as getIndexInfo() */
  @Override
  public ResultSet getPrimaryKeys(String catalog,
          String schema, String table) throws SQLException 
  {
    ResultSet rs = null;
    /***
    PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
            + "TABLE_CATALOG TABLE_CAT, "
            + "TABLE_SCHEMA TABLE_SCHEM, "
            + "TABLE_NAME, "
            + "COLUMN_NAME, "
            + "ORDINAL_POSITION KEY_SEQ, "
            + "IFNULL(CONSTRAINT_NAME, INDEX_NAME) PK_NAME "
            + "FROM INFORMATION_SCHEMA.INDEXES "
            + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
            + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
            + "AND TABLE_NAME = ? "
            + "AND PRIMARY_KEY = TRUE "
            + "ORDER BY COLUMN_NAME");
    prep.setString(1, getCatalogPattern(catalogPattern));
    prep.setString(2, "\\");
    prep.setString(3, getSchemaPattern(schemaPattern));
    prep.setString(4, "\\");
    prep.setString(5, tableName);
     ***/
    List<String> listConditions = new ArrayList<String>();
    if (catalog != null)
      listConditions.add("TABLE_CATALOG = "+SqlLiterals.formatStringLiteral(catalog));
    if (schema != null)
      listConditions.add("TABLE_SCHEMA = "+SqlLiterals.formatStringLiteral(schema));
    if (table != null)
      listConditions.add("TABLE_NAME = "+SqlLiterals.formatStringLiteral(table));
    StringBuilder sbSql = new StringBuilder("SELECT\r\n");
    sbSql.append("TABLE_CATALOG AS TABLE_CAT,\r\n");
    sbSql.append("TABLE_SCHEMA AS TABLE_SCHEM,\r\n");
    sbSql.append("TABLE_NAME,\r\n");
    sbSql.append("COLUMN_NAME,\r\n");
    sbSql.append("ORDINAL_POSITION AS KEY_SEQ,\r\n");
    sbSql.append("INDEX_NAME AS PK_NAME\r\n");
    sbSql.append("FROM INFORMATION_SCHEMA.INDEXES\r\n");
    sbSql.append("WHERE PRIMARY_KEY = TRUE\r\n");
    for (Iterator<String> iterCondition = listConditions.iterator(); iterCondition.hasNext(); )
      sbSql.append(" AND "+iterCondition.next()+"\r\n");
    sbSql.append("ORDER BY COLUMN_NAME");
    Statement stmt = getConnection().createStatement();
    rs = stmt.unwrap(Statement.class).executeQuery(sbSql.toString());
    return rs;
  } /* getPrimaryKeys */
  
} /* H2DatabaseMetaData */
