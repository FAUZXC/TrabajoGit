
package capaDatos;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/** Clase que gestiona el acceso a la Base de Datos.
 *
 * @author Alberto Esteves Correia
 */
public class DataAccessObject {


    private static final String DRIVER = "com.mysql.jdbc.Driver";

    /** Contiene la direcci贸n de la Base de Datos.
     *  Se inicializa al comenzar la aplicaci贸n.
     *  El usuario podr谩 modificarlo en el men煤 de configuraci贸n.
     */    
    public static String URL = "localhost";

    /** Contiene el usuario para la conexi贸n a la Base de Datos.
     *  Se inicializa al comenzar la aplicaci贸n.
     *  El usuario podr谩 modificarlo en el men煤 de configuraci贸n.
     */
    public static String USER;

    /** Contiene la contrase帽a para la conexi贸n a la Base de Datos.
     *  Se inicializa al comenzar la aplicaci贸n.
     *  El usuario podr谩 modificarlo en el men煤 de configuraci贸n.
     */
    public static String PASS;

    private static DataAccessObject dataAccessObject=null;
    private PreparedStatement statement = null;
    private Connection connection;

    /** M茅todo que nos devuelve el objeto dataAccessObject est谩tico que tiene la clase.
     * La primera vez que se llama a getDataAccessObjectConnected() crea el objeto DataAccessObject
     * y har谩 la conexi贸n a la BD. En cualquier caso se hace 鈥渞eturn DataAccessObject鈥� para que
     * podamos usar el objeto
     *
     * @return dataAccessObject
     */
    public static DataAccessObject getDataAccessObjectConnected(){
		if (dataAccessObject==null){
                    dataAccessObject = new DataAccessObject();
		}
		dataAccessObject.connect();
	
            return dataAccessObject;

    } // fin del m茅todo getDataAccessObjectConnected


    /** M茅todo que devuelve el objeto statement con el que realizaremos
     * las consultas sql.
     *
     * @param sql contiene la consulta sql a realizar
     *
     * @return statement 
     */

    public PreparedStatement getPreparedStatement(String sql){
        try {
            this.statement = connection.prepareStatement(sql);
            return statement;
        }
        catch (SQLException ex) {
            throw new RuntimeException("Problema al obtener el prepared statement"
                                     + " el sql es: "+sql);
        }
    } // fin del m茅todo getPreparedStatement

    private DataAccessObject(){}

    /** Este m茅todo se encarga de cargar el Driver MySQL y de realizar
     *  la conexi贸n a la Base de Datos. Si ocurre alg煤n error al cargar el Driver
     *  o al intentar conectar a la Base de Datos, el m茅todo lanzar谩 una excepci贸n.
     *
     */
    private void connect(){
	try {
		Class.forName(DRIVER);
		connection = DriverManager.getConnection(URL, USER, PASS);
                connection.setAutoCommit(false);
	} catch (ClassNotFoundException e) {
		throw new RuntimeException("problemas de driver");
	} catch (SQLException e) {
		throw new RuntimeException("Ha ocurrido un error al conectar con la Base de Datos");
	}
    } // fin del m茅todo connect


   /** M茅todo que ejecuta la consulta SQL para insertar, borrar o actualizar.
     * Si ocurre alg煤n error, lanzar谩 una excepci贸n.
     */
    public void actualizar (){
	try{
            
            this.statement.executeUpdate();
	} catch (SQLException e){
            System.out.println(e.getSQLState());
            e.printStackTrace();
            System.out.println(e.getMessage());
            throw new RuntimeException("Error de actualizaci贸n ");
	}
    } // fin del m茅todo actualizar


    /** M茅todo que libera los recursos de la Base de Datos y se encarga de
     *  cerrar la conexi贸n a la Base de Datos.
     *  Si ocurre alg煤n error, lanzar谩 una excepci贸n.
     */
    public void close () {
        try {
            statement.close();
            this.closeConnection();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Problema al cerrar la conexi贸n con la Base de Datos");
        }
    } // fin del m茅todo close
	
	
    /** M茅todo que realiza la entrega (commit) de la sentencia sql y 
     *  cierra la conexi贸n a la Base de Datos.
     *  Si ocurre alg煤n error, lanzar谩 una excepci贸n.
     */
    private void closeConnection () {
        try {
            connection.commit();
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException ex) {
 //           this.rollback();
            throw new RuntimeException("Problema al cerrar la conexi贸n");
        }
    } // fin del m茅todo closeConnection


    /** M茅todo que aborta la transaci贸n y devuelve cualquier valor
     *  que fuera modificado a sus valores anteriores.
     *
     */
    public void rollback () {
        try {
            statement.close();
            System.out.println("Rollback 1");
            connection.rollback();
            System.out.println("Rollback 2");
            connection.close();
        } catch (SQLException ex) {
            System.out.println("Problema al hacer rollback");
        }
    } // fin del m茅todo rollback

} // fin de la clase DataAccessObject