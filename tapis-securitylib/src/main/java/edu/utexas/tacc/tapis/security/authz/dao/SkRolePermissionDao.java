package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkRolePermission;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkRolePermissionDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkRolePermissionDao.class);
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // The database datasource provided by clients.
  private final DataSource _ds;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  /** This class depends on the calling code to provide a datasource for
   * db connections since this code in not part of a free-standing service.
   * 
   * @param dataSource the non-null datasource 
 * @throws TapisException 
   */
  public SkRolePermissionDao() throws TapisException
  {
      _ds = SkDaoUtils.getDataSource();
  }
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getRolePermissions:                                                    */
  /* ---------------------------------------------------------------------- */
  public List<SkRolePermission> getRolePermissions() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<SkRolePermission> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_SKROLEPERMISSION;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkRolePermission obj = populateSkRolePermission(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateSkRolePermission(rs);
          }
          
          // Close the result and statement.
          rs.close();
          pstmt.close();
    
          // Commit the transaction.
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "SkRolePermissions", "all", e.getMessage());
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      finally {
          // Always return the connection back to the connection pool.
          try {if (conn != null) conn.close();}
            catch (Exception e) 
            {
              // If commit worked, we can swallow the exception.  
              // If not, the commit exception will be thrown.
              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
              _log.error(msg, e);
            }
      }
      
      return list;
  }

  /* ---------------------------------------------------------------------- */
  /* assignPermission:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Assign a named child role to the parent role with the specified id. It
   * is expected that all information other than the childRoleName was extracted
   * from the parent role populated from the database. Otherwise, it is possible
   * to attempt assigning a child role from one tenant to a parent in another.
   * The query will filter out such attempts and an exception will be thrown
   * because no records will be inserted into the sk_role_tree table.
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0.  
   * 
   * @param tenant the tenant
   * @param user the creating user
   * @param roleId the role to which the permission will be assigned
   * @param permissionName the name of the permission to be assigned to the role
   * @return number of rows affected (0 or 1)
   * @throws TapisException if a single row is not inserted
   */
  public int assignPermission(String tenant, String user, int roleId, 
                              String permissionName) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(permissionName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "permissionName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (roleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "assignPermission", "roleId", roleId);
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      int rows = 0;
      try
      {
          // Get a database connection.
          conn = getConnection();

          // Set the sql command.
          String sql = SqlStatements.ROLE_ADD_PERMISSION_BY_NAME;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setInt(1, roleId);
          pstmt.setString(2, user);
          pstmt.setString(3, user);
          pstmt.setString(4, tenant);
          pstmt.setString(5, permissionName);
          pstmt.setInt(6, roleId);

          // Issue the call. 0 rows will be returned when a duplicate
          // key conflict occurs--this is not considered an error.
          rows = pstmt.executeUpdate();

          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          // Log the exception.
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_role_permission");
          _log.error(msg, e);
          throw TapisUtils.tapisify(e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
          if (conn != null)
              try {conn.close();}
              catch (Exception e)
              {
                  // If commit worked, we can swallow the exception.
                  // If not, the commit exception will be thrown.
                  String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                  _log.error(msg, e);
              }
      }
      
      return rows;
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getConnection:                                                         */
  /* ---------------------------------------------------------------------- */
  private Connection getConnection()
    throws TapisException
  {
    // Get the connection.
    Connection conn = null;
    try {conn = _ds.getConnection();}
      catch (Exception e) {
        String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
        _log.error(msg, e);
        throw new TapisDBConnectionException(msg, e);
      }
    
    return conn;
  }

  /* ---------------------------------------------------------------------- */
  /* populateSkRolePermission:                                              */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkRolePermission object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkRolePermission object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private SkRolePermission populateSkRolePermission(ResultSet rs)
   throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;
    
    try {
      // Return null if the results are empty or exhausted.
      // This call advances the cursor.
      if (!rs.next()) return null;
    }
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    
    // Populate the SkRolePermission object using table definition field order,
    // which is the order specified in all calling methods.
    SkRolePermission obj = new SkRolePermission();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setRoleId(rs.getInt(3));
        obj.setPermissionId(rs.getInt(4));
        obj.setCreated(rs.getTimestamp(5).toInstant());
        obj.setCreatedby(rs.getString(6));
        obj.setUpdated(rs.getTimestamp(7).toInstant());
        obj.setUpdatedby(rs.getString(8));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
}
