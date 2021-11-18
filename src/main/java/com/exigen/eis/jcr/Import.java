/* Copyright © 2016 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.exigen.eis.jcr;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.exigen.cm.jackrabbit.util.ISO9075;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class Import extends DefaultHandler {

	private static int id;

	private final static Logger LOG = LoggerFactory.getLogger(Import.class);

	private long processedDate;
	
	private static class PathKey{
		private String [] path;
		Integer hashCode;
		PathKey(String path){
			this.path = path.split("/");
		}

		PathKey(String[] path){
			this.path = path;
		}

		@Override
		public int hashCode() {		
			if(hashCode != null){
				return hashCode;
			}
			int hashCodeVal = 0;
			for(String e : path){
				hashCodeVal += 11*e.hashCode();
			}
			hashCode = hashCodeVal;
			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			PathKey other = (PathKey) obj;
			if(path.length != other.path.length	){
				return false;
			}
			for(int i = 0; i < path.length; i ++){
				if( path[i] != other.path[i]){
					if( !path[i].equals(other.path[i])){
						return false;
					}
				}
			}

			return true;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			for(String e : path){
				builder.append("/");
				builder.append(e);				
			}			
			return builder.toString();
		}

	}

	private static class Element {

		private Element parent;
		private List<Element> children = new ArrayList<Element>();
		private String name;
		private StringBuilder value = new StringBuilder();

		String getAttribute(String name){
			for(Element e : children){
				if(name.equals(e.name)){
					return e.value.toString();
				}				
			}			
			return null;
		}

		Element getElement(String name){
			for(Element e : children){
				if(name.equals(e.name)){
					return e;
				}				
			}			
			return null;
		}



		Element(Element parent){
			this.parent = parent;
		}

		public void setName(String name) {
			this.name = name;

		}

		@Override
		public String toString() {
			if(children.size() == 0){
				return "<" + name + ">" + value + "</" + name + ">\n";
			}else {
				StringBuilder builder = new StringBuilder("<" + name + ">\n");
				for(Element e : children){
					builder.append("\t" + e.toString());		
				}	
				return builder.append("</" +  name + ">\n").toString();
			}
		}

	}

	private Connection connection;
	private Element currentElement;
	private String repository;
	private Map<String,List<String>> docTypeMapping = new HashMap<String, List<String>>();
	private SimpleDateFormat  dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private int batchSize;
	private PreparedStatement insertDocumentStatement;
	private PreparedStatement insertNodeEventStatement;
	private PreparedStatement insertNodeStatement;
	private PreparedStatement insertEventStatement;
	private PreparedStatement insertPartitionStatement;
	private PreparedStatement insertDocPartitionStatement;
	private HashSet<String> partitions = new HashSet<String>();
	
	private final int batchSizeProperty = Integer.valueOf(System.getProperty("import.batchSize", "1000"));
	private final int commitSizeProperty = Integer.valueOf(System.getProperty("import.commitSize", "10000"));


	public Import(Connection connection) throws SQLException {
		this.connection = connection;

		clearTables();
		prepareBatch(connection);
		
		if(connection.getMetaData().getURL().contains("sqlserver")){
			LOG.info("SET NOCOUNT ON");
			connection.createStatement().execute("SET NOCOUNT ON");
		}
	}

	private void prepareBatch(Connection connection) throws SQLException {
		String sql = "INSERT /*+APPEND*/ INTO DOC_DOCUMENT_EVENT" +
				"(EVENT_ID, NODE_ID, DOCUMENT_TYPE_ID,OWNER,CONTENT_LENGHT,CONTENT_TYPE, DOC_COMMENT,CONTENT_ID )"+
				"VALUES(?,?,?,?,?,?,?,?)";
		this.insertDocumentStatement = connection.prepareStatement(sql);	

		sql = "INSERT /*+APPEND*/ INTO DOC_NODE_EVENT (NODE_ID,EVENT_ID,PARENT_ID, NAME,SOURCE_URL, NODE_TYPE)VALUES (?,?,?,?,?,?)";
		this.insertNodeEventStatement = connection.prepareStatement(sql);

		sql = "INSERT /*+APPEND*/ INTO DOC_NODE (NODE_ID, LAST_EVENT_ID, NODE_TYPE)VALUES (?, ?, ?)";
		this.insertNodeStatement = connection.prepareStatement(sql);

		sql = "INSERT /*+APPEND*/ INTO DOC_EVENT(EVENT_ID,EVENT_TYPE,CHANGE_TIME,PERFORMER,REPOSITORY_ID)" +
				"VALUES (?,?,?,?,?)";
		this.insertEventStatement = connection.prepareStatement(sql);

		sql = "INSERT /*+APPEND*/ INTO DOC_PARTITION( CREATE_EVENT_ID,ARCHYVE_EVENT_ID,PARTITION_NAME,REPOSITORY_ID)VALUES(?,?,?,?)";				
		this.insertPartitionStatement = connection.prepareStatement(sql);

		sql = "INSERT /*+APPEND*/ INTO DOC_DOCUMENT_PARTITION( NODE_ID, PARTITION_NAME )VALUES(?,?)";
		this.insertDocPartitionStatement = connection.prepareStatement(sql);
	}

	@Override
	public void startDocument() throws SAXException {
		try{
			String exitsSQL = "SELECT COUNT(*) FROM DOC_REPOSITORY WHERE REPOSITORY_ID = ?";
			PreparedStatement exits = connection.prepareStatement(exitsSQL );
			try{
				exits.setString(1, repository);
				ResultSet rs = exits.executeQuery();
				try{
					rs.next();
					if(rs.getInt(1) == 0){
						String sql = "INSERT INTO DOC_REPOSITORY(REPOSITORY_ID,DESCRIPTION)VALUES(?,'JCR Migration')";
						PreparedStatement statement = connection.prepareStatement(sql);
						statement.setString(1, repository);
						try{
							statement.execute();
						}finally{
							statement.close();
						}
					}
				}finally{
					rs.close();
				}
			}finally{
				exits.close();
			}
		}catch (Exception e) {
			throw new SAXException(e);
		}
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

		Element newElement = new Element(currentElement);
		newElement.setName(qName);
		if(currentElement != null){
			if(!Arrays.asList("documentType","extDocument","document").contains(qName)){
				currentElement.children.add(newElement);
			}
		}

		currentElement = newElement;

	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {	
		try {
			if("documentType".equals(qName)){			
				insertDocumentType(currentElement);			
			}else if ("folders".equals(qName)){
				insertFolders(currentElement);
			}else if("documents".equals(qName) ||  "extDocuments".equals(qName)){
				executeDocumentBatch();
				connection.commit();
				LOG.info("Executed batch " + batchSize );
				LOG.info("Documents processing finished.");
			}else if ("document".equals(qName) || "extDocument".equals(qName)){	
				insertDocument(currentElement,qName.equals("extDocument"));

				batchSize++;

				if (processedDate == 0) {
					processedDate = System.currentTimeMillis();
				}
				if(batchSize % batchSizeProperty == 0){
					executeDocumentBatch();
					
					long currentDate = System.currentTimeMillis();
					long diff = currentDate - processedDate;
					processedDate = currentDate;
					LOG.info("Executed batch " + batchSize + " ("+ batchSizeProperty + " in " + Migrate.getDurationBreakdown(diff) + ")" );
				}
				if(batchSize % commitSizeProperty == 0){
					connection.commit();					
				}

			}

		} catch (Exception e) {
			throw new SAXException(e);
		}

		currentElement = currentElement.parent;
	}

	private void executeDocumentBatch() throws SQLException {

		insertEventStatement.executeBatch();
		insertEventStatement.close();
		
		insertNodeStatement.executeBatch();
		insertNodeStatement.close();
		
		insertNodeEventStatement.executeBatch();
		insertNodeEventStatement.close();
		
		insertDocumentStatement.executeBatch();
		insertDocumentStatement.close();
		
		insertPartitionStatement.executeBatch();
		insertPartitionStatement.close();
		
		insertDocPartitionStatement.executeBatch();
		insertDocPartitionStatement.close();
		
		connection.clearWarnings();
		
		prepareBatch(connection);
		
		
	}

	private void insertDocument( Element document, boolean external) throws SQLException, ParseException {

		String eventId = "m" + Integer.toHexString(id++);
		insertEvent(eventId, "DEPLOY", 
				parseDate(document.getAttribute("modifiedDate")), 
				document.getAttribute("modifiedBy"));

		String nodeId = document.getAttribute("uuid");
		String contentId = document.getAttribute("contentId");
		String partition = null;
		
		if(contentId != null){
			partition = contentId.substring(0,contentId.lastIndexOf('/'));
			contentId = contentId.substring(contentId.lastIndexOf('/') + 1);
		}
		
		String nodeType = external ? "URL" : "DOCUMENT";		
		insertNode(eventId, nodeId, nodeType);	

		insertNodeEvent(eventId, 
				document.getAttribute("folderId"), 
				nodeId, document.getAttribute("name"), 
				document.getAttribute("resourceUrl"),
				nodeType);

		if(!external){
			int i = 1;
			insertDocumentStatement.setString(i++, eventId);
			insertDocumentStatement.setString(i++, nodeId);
			insertDocumentStatement.setString(i++, document.getAttribute("documentTypeCode"));
			insertDocumentStatement.setString(i++, document.getAttribute("entityId"));
			String size =  document.getAttribute("contentSize");		
			insertDocumentStatement.setLong(i++, Long.parseLong(size));		
			insertDocumentStatement.setString(i++, document.getAttribute("mimeType"));
			insertDocumentStatement.setString(i++, document.getAttribute("comment"));	
			insertDocumentStatement.setString(i++, contentId);
			insertDocumentStatement.addBatch();

			if(!partitions.contains(partition)){
				i = 1;
				insertPartitionStatement.setString(i++, eventId);
				insertPartitionStatement.setString(i++, eventId);
				insertPartitionStatement.setString(i++, partition);
				insertPartitionStatement.setString(i++, repository);
				insertPartitionStatement.addBatch();
				partitions.add(partition);
			}

			i = 1;		
			insertDocPartitionStatement.setString(i++, nodeId);
			insertDocPartitionStatement.setString(i++, partition);
			insertDocPartitionStatement.addBatch();

		}


	}

	private Timestamp parseDate(String value) throws ParseException{		
		return new Timestamp(dateFormat.parse(value).getTime());

	}

	private void insertFolders(Element element) throws SQLException {

		String eventId = UUID.randomUUID().toString();
		insertEvent(eventId,"DEPLOY",new Timestamp(System.currentTimeMillis()),"ipbSys");
		insertEventStatement.executeBatch();

		Map<PathKey,Element> map = new HashMap<PathKey, Element>();
		for(Element folder : element.children){
			map.put(new PathKey(folder.getAttribute("path")), folder);
		}

		HashSet<PathKey> inserted = new HashSet<PathKey>();

		Collections.sort(element.children, new Comparator<Element>() {
			public int compare(Element o1, Element o2) {
				return o1.getAttribute("path").length() - o2.getAttribute("path").length();
			}
		});
		
		for(Element folder : element.children){

			//split, insert, add to insert map
			String path = folder.getAttribute("path");
			PathKey key = new PathKey(path);
			String[] pathAray = path.split("/");
			String[] parentPathArray = new String[pathAray.length - 1];
			System.arraycopy(pathAray, 0, parentPathArray, 0, pathAray.length - 1);			
			if(!inserted.contains(key)){

				String parentId = null;
				if(parentPathArray.length > 0){
					Element parent =  map.get(new PathKey(parentPathArray));
					parentId = parent.getAttribute("id");
				}else {
					List<String> allTypes = docTypeMapping.get(pathAray[0]);
					if(allTypes != null){
						for(String type : allTypes){
							insertDoctTypeMap(eventId,type,folder.getAttribute("id"));
						}
					}
				}
				insertFolder(eventId,parentId, folder);
				inserted.add(key);
			}
		}

	}


	private void insertFolder(String eventId, String parentId, Element folder) throws SQLException {

		String nodeId = folder.getAttribute("id");
		String nodeType = "FOLDER";
		insertNode(eventId, nodeId, nodeType);
		insertNodeStatement.executeBatch();

		String[] path = folder.getAttribute("path").split("/");
		String name = path[path.length - 1];
		String url = null;

		insertNodeEvent(eventId, parentId, nodeId, name, url, nodeType);
		insertNodeEventStatement.executeBatch();

		insertPrivileges(eventId,folder);
		insertAllwedTypes(eventId,folder);

	}

	private void insertNodeEvent(String eventId, String parentId,
			String nodeId, String name, String url, String type) throws SQLException {

		int i = 1;
		insertNodeEventStatement.setString(i++, nodeId);
		insertNodeEventStatement.setString(i++, eventId);
		insertNodeEventStatement.setString(i++, parentId);
		insertNodeEventStatement.setString(i++, ISO9075.encode(name));
		insertNodeEventStatement.setString(i++, url);
		insertNodeEventStatement.setString(i++, type);
		insertNodeEventStatement.addBatch();

	}

	private void insertNode(String eventId, String nodeId,String type) throws SQLException {		
		int i = 1;
		insertNodeStatement.setString(i++, nodeId);
		insertNodeStatement.setString(i++, eventId);
		insertNodeStatement.setString(i++, type);
		insertNodeStatement.addBatch();		

	}

	private void insertAllwedTypes(String eventId, Element folder) throws SQLException {
		for(Element e : folder.getElement("allowedTypes").children){
			insertDoctTypeMap(eventId,e.value.toString(),folder.getAttribute("id"));
		}

	}

	private void insertDoctTypeMap(String eventId, String docType, String folderId) throws SQLException {
		String sql = "INSERT INTO DOC_TYPE_MAP_EVENT(EVENT_ID,DOCUMENT_TYPE_ID,NODE_ID)VALUES(?,?,?)";
		PreparedStatement statement = connection.prepareStatement(sql);
		try{
			int i = 1;
			statement.setString(i++, eventId);
			statement.setString(i++, docType);
			statement.setString(i++, folderId);
			statement.execute();
		}finally{
			statement.close();
		}

	}

	private void insertPrivileges(String eventId, Element folder) throws SQLException {
		for(Element e : folder.getElement("writePrivileges").children){
			insertPrivilege(eventId,folder.getAttribute("id"),"WRITE",e.value.toString());
		}

		for(Element e : folder.getElement("readPrivileges").children){
			insertPrivilege(eventId,folder.getAttribute("id"),"READ",e.value.toString());
		}	

	}

	private void insertPrivilege(String eventId, String folderId,
			String action, String value) throws SQLException {

		String sql = "INSERT INTO DOC_NODE_PRIVILEGE_EVENT(EVENT_ID,PRIVILEGE_ID,NODE_ID, ACTION)VALUES(?,?,?,?)";
		PreparedStatement statement = connection.prepareStatement(sql);
		try{
			int i = 1;
			statement.setString(i++, eventId);
			statement.setString(i++, value);
			statement.setString(i++, folderId);
			statement.setString(i++, action);

			statement.execute();
		}finally{
			statement.close();
		}


	}

	private void insertDocumentType(Element docType) throws SQLException {

		String eventId = UUID.randomUUID().toString();
		String docTypeId = docType.getAttribute("uuid");		
		String entityType = docType.getAttribute("entityType");

		List<String> list = docTypeMapping.get(entityType);
		if(list == null){
			list = new ArrayList<String>();
			docTypeMapping.put(entityType, list);
		}
		list.add(docTypeId);

		insertEvent(eventId,"DEPLOY",new Timestamp(System.currentTimeMillis()),"ipbSys");
		insertEventStatement.executeBatch();

		String sql = "INSERT INTO DOC_DOCUMENT_TYPE "+
				" ( DOCUMENT_TYPE_ID,NAME,CODE,REPOSITORY_ID,ADD_EVENT_ID,ENTITY_TYPE )" +
				" VALUES (?,?,?,?,?,? )";	

		PreparedStatement statement = connection.prepareStatement(sql);
		try {
			int i = 1;			
			statement.setString(i++, docTypeId);
			statement.setString(i++, docType.getAttribute("name"));
			statement.setString(i++, docType.getAttribute("code"));
			statement.setString(i++, repository);			
			statement.setString(i++, eventId);
			statement.setString(i++, entityType);

			statement.executeUpdate();

		}finally{
			statement.close();
		}

		Element labels = docType.getElement("labels");
		if(labels != null){
			sql = " INSERT INTO DOC_TYPE_LABEL(EVENT_ID,DOCUMENT_TYPE_ID,LOCALE_CD,LABEL)VALUES(?,?,?,?) ";
			statement = connection.prepareStatement(sql);
			try {
				for(Element e : labels.children){
					int i = 1;			
					statement.setString(i++, eventId);
					statement.setString(i++, docTypeId);
					statement.setString(i++, e.getAttribute("locale"));
					statement.setString(i++, e.getAttribute("text"));
					statement.executeUpdate();
				}
			}finally{
				statement.close();
			}
		}
	}

	private void insertEvent(String eventId,String eventType,Timestamp changeTime,String performer) throws SQLException{

		int i = 1;
		insertEventStatement.setString(i++, eventId);
		insertEventStatement.setString(i++, eventType);
		insertEventStatement.setTimestamp(i++, changeTime);
		insertEventStatement.setString(i++, performer);
		insertEventStatement.setString(i++, repository);
		insertEventStatement.addBatch();


	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		if(!Arrays.asList("documentTypes","extDocuments","documents").contains(currentElement.name)){
			currentElement.value.append(ch,start,length);		
		}
	}


	private static void setupDriver() throws ClassNotFoundException {
		if(System.getProperty("dbDriver") != null){
			Class.forName(System.getProperty("dbDriver"));
		}else if(System.getProperty("dbUrl").contains("oracle")){			
			Class.forName("oracle.jdbc.OracleDriver");				
		}else if ( System.getProperty("dbUrl").startsWith("jdbc:jtds:") ){
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		}else if (System.getProperty("dbUrl").contains("sqlserver")){
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		}
	}

	void setRepository(String repository) {
		this.repository = repository;		
	}


	private void checkTables(String[] tables) throws SQLException {
		boolean emptyTables = true;
		for(String tableName : tables){
			Statement countStatement = connection.createStatement();
			try {
				ResultSet documentCountResultSet = countStatement.executeQuery("select count(*) from " + tableName);
				try {
					documentCountResultSet.next();
					int count = documentCountResultSet.getInt(1);
					LOG.info("Total records in " + tableName + " table: " + count);
					if (count > 0) {
						emptyTables = false;
					}
				}
				finally {
					documentCountResultSet.close();
				}
			}
			finally {
				countStatement.close();
			}
		}
		
		if (!emptyTables) {
			LOG.info("CMIS tables are not empty. Use forceCMISmigration flag to force migration. It will delete existing data.");
			throw new IllegalStateException("CMIS tables are not empty. Use forceCMISmigration flag to force migration. It will delete existing data.");
		}
	}
	
	private void clearTables() throws SQLException{

		
		String[] tables = new String[]{
				"DOC_DOCUMENT_EVENT","DOC_NODE_EVENT","DOC_DOCUMENT_PARTITION","DOC_LABEL","DOC_NODE_PRIVILEGE_EVENT","DOC_TYPE_MAP_EVENT",
				"DOC_PARTITION","DOC_TYPE_LABEL"};
		
		if (!System.getProperties().containsKey("forceCMISmigration")) {
			checkTables(tables);
		}
		LOG.info("Clearing tables...");
		StringBuilder clearTables = new StringBuilder("(");

		for(String tableName : tables){
			clearTables.append("'");
			clearTables.append(tableName);
			clearTables.append("',");
		}

		clearTables.append("'')");		

		String oracle = 
				"begin " +		
						"     for c in (select table_name from user_tables where table_name in" + clearTables+ ") loop " +
						"          execute immediate ('truncate table '||c.table_name); " +
						"     end loop; " +
						"end;"; 

		StringBuilder sqlServer = new StringBuilder();

		for(String tableName : tables){
			sqlServer.append("TRUNCATE TABLE "+ tableName + "\n");			
		}				
		Statement statement = connection.createStatement();
		try{
			if(connection.getMetaData().getURL().toLowerCase().contains("oracle")){
				statement.execute(oracle);					
			}else {
				statement.execute(sqlServer.toString());
			}
			statement.execute("DELETE FROM DOC_DOCUMENT_TYPE");
			statement.execute("DELETE FROM DOC_NODE");
			statement.execute("DELETE FROM DOC_EVENT");
			connection.commit();
		}finally{
			statement.close();
		}

	}
	
	
	private void toggleIndexMSSQL(boolean enable) throws SQLException{
		

		String action = enable ? " REBUILD  WITH ( SORT_IN_TEMPDB = ON, ONLINE = OFF)" : " DISABLE";

		String sql = "SELECT  i.name, o.name FROM  sys.indexes i JOIN    sys.objects o    ON i.object_id = o.object_id " +
				"WHERE i.type_desc = 'NONCLUSTERED'  AND o.type_desc = 'USER_TABLE'  and o.name in " +
				"('DOC_DOCUMENT_EVENT','DOC_NODE_EVENT','DOC_DOCUMENT_PARTITION','DOC_LABEL NOCHECK', 'DOC_NODE_PRIVILEGE_EVENT',"+
				"'DOC_TYPE_MAP_EVENT','DOC_PARTITION','DOC_TYPE_LABEL','DOC_DOCUMENT_TYPE',  'DOC_NODE','DOC_EVENT'); ";

		Statement statement = connection.createStatement();
		try{
			ResultSet rs = statement.executeQuery(sql);
			try{
				while(rs.next()){
					Statement index = connection.createStatement();
					try{
						index.execute("ALTER INDEX " + rs.getString(1) + " ON " + rs.getString(2) + action);
					}finally{
						index.close();
					}
				}
			}finally{
				rs.close();
			}
		}finally{
			statement.close();
		}

	}
	
	private void toggleIndexOracle(boolean enable) throws SQLException{
		

		String action = enable ? " REBUILD" : " UNUSABLE";

		String sql = " select index_name,table_name from user_indexes where table_name like 'DOC?_%' escape  '?'  ";

		Statement statement = connection.createStatement();
		try{
			ResultSet rs = statement.executeQuery(sql);
			try{
				while(rs.next()){
					Statement index = connection.createStatement();
					try{
						index.execute("ALTER INDEX " + rs.getString(1) +  action);
					}finally{
						index.close();
					}
				}
			}finally{
				rs.close();
			}
		}finally{
			statement.close();
		}

	}
	
	
	
	private void preprocess() throws SQLException {
		LOG.info("Disabling constraints and indexes.");
		String[] tables = new String[]{"DOC_DOCUMENT_EVENT","DOC_DOCUMENT_PARTITION","DOC_LABEL","DOC_NODE_EVENT","DOC_NODE_PRIVILEGE_EVENT","DOC_PARTITION","DOC_TYPE_LABEL","DOC_TYPE_MAP_EVENT",
				"DOC_DOCUMENT_TYPE","DOC_NODE","DOC_EVENT","DOC_EVENT_TYPE","DOC_REPOSITORY"};

		Statement statement = connection.createStatement();
		try{
			if(connection.getMetaData().getURL().toLowerCase().contains("oracle")){
				createConstraintsOracleProcedure();
				for(String tableName : tables){
					statement.execute("{call sp_toggle_constraints(false, '"+ tableName + "')}");
				}
				statement.execute("alter session set skip_unusable_indexes = true");
				toggleIndexOracle(false);
			}else {
				StringBuilder sql = new StringBuilder();
				for(String tableName : tables){
					sql.append("ALTER TABLE "+ tableName + " NOCHECK CONSTRAINT all\n");
				}
				
				
				statement.execute(sql.toString());
				toggleIndexMSSQL(false);
			}
			connection.commit();
		}finally{
			statement.close();
			LOG.info("Constraints and indexes disabled.");
		}
	}
	
	private void postprocess() throws SQLException {
		LOG.info("Enabling constraints and indexes.");
		String[] tables = new String[]{"DOC_REPOSITORY","DOC_EVENT_TYPE","DOC_EVENT","DOC_NODE","DOC_DOCUMENT_TYPE","DOC_TYPE_MAP_EVENT","DOC_TYPE_LABEL","DOC_PARTITION",
				"DOC_NODE_PRIVILEGE_EVENT","DOC_NODE_EVENT","DOC_LABEL","DOC_DOCUMENT_PARTITION","DOC_DOCUMENT_EVENT"};
		
		Statement statement = connection.createStatement();
		try{
			if(connection.getMetaData().getURL().toLowerCase().contains("oracle")){
				for(String tableName : tables){
					statement.execute("{call sp_toggle_constraints(true, '"+ tableName + "')}");
				}
				toggleIndexOracle(true);
				dropConstraintsOracleProcedure();
			}else {
				StringBuilder sql = new StringBuilder();
				for(String tableName : tables){
					sql.append("ALTER TABLE "+ tableName + " WITH NOCHECK CHECK CONSTRAINT all\n");	
				}				
				
				statement.execute(sql.toString());
				
				toggleIndexMSSQL(true);
			}
			connection.commit();
		}finally{
			statement.close();
			LOG.info("Constraints and indexes enabled.");
		}
	}

	private void createConstraintsOracleProcedure() throws SQLException {
        Statement statement = connection.createStatement();
        try {
            statement.execute("CREATE OR REPLACE PROCEDURE sp_toggle_constraints (in_enable in boolean, in_table IN VARCHAR2) is \n" +
            		"begin \n" +
            		" \n" +
            		"  if in_enable = false then \n" +
            		"    dbms_output.put_line('Disabling constrains on ' || in_table); \n" +
            		"    for i in  \n" +
            		"    ( \n" +
            		"      select constraint_name, table_name  \n" +
            		"      from user_constraints  \n" +
            		"      where  \n" +
            		"      --constraint_type ='R' and \n" +
            		"      status = 'ENABLED' \n" +
            		"      and table_name = in_table \n" +
            		"    ) LOOP \n" +
            		"      dbms_output.put_line('alter table '||i.table_name||' disable constraint '||i.constraint_name||''); \n" +
            		"      execute immediate 'alter table '||i.table_name||' disable constraint '||i.constraint_name||''; \n" +
            		"    end loop; \n" +
            		"  else \n" +
            		"    dbms_output.put_line('Enabling constrains on ' || in_table); \n" +
            		"    for i in  \n" +
            		"    ( \n" +
            		"      select constraint_name, table_name  \n" +
            		"      from user_constraints  \n" +
            		"      where  \n" +
            		"      --constraint_type ='R' and \n" +
            		"      status <> 'ENABLED' \n" +
            		"      and table_name = in_table \n" +
            		"    ) LOOP \n" +
            		"      dbms_output.put_line('alter table '||i.table_name||' enable novalidate constraint '||i.constraint_name||''); \n" +
            		"      execute immediate 'alter table '||i.table_name||' enable novalidate  constraint '||i.constraint_name||''; \n" +
            		"    end loop; \n" +
            		"  end if; \n" +
            		"end; ");
            connection.commit();
        } finally {
        	statement.close();
        }
    }
	
	private void dropConstraintsOracleProcedure() throws SQLException {
		Statement statement = connection.createStatement();
		try {
			statement.execute("DROP PROCEDURE sp_toggle_constraints");
			connection.commit();
		} finally {
			statement.close();
		}
	}
	
	public static void main(String[] args) throws Exception {

		String fileName = System.getProperty("migration.file","jcr-cmis-migration.xml.gz");
		Date startDate = Calendar.getInstance().getTime();
		LOG.info("Import started: " + startDate.toString());
		LOG.info("Importing from file: " + fileName);
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser = factory.newSAXParser();
		InputStream inputStream = new GZIPInputStream( new FileInputStream(fileName) );

		try{

			setupDriver();
			String dbUrl = System.getProperty("dbUrl");
			if (dbUrl.startsWith("jdbc:sqlserver") && !dbUrl.contains("selectMethod")) {
				if (!dbUrl.endsWith(";")) {
					dbUrl = dbUrl + ";";
				}
				dbUrl = dbUrl + "selectMethod=cursor";
			}
			Connection connection = DriverManager.getConnection(dbUrl,
					System.getProperty("dbLogin"), System.getProperty("dbPassword"));
			try{
				connection.setAutoCommit(false);
				Import handler = new Import(connection);
				handler.preprocess();
				handler.setRepository(System.getProperty("repository","default"));
				saxParser.parse(inputStream,handler);
				connection.commit();
				handler.postprocess();
			}finally{
				connection.rollback();
				connection.close();
			}			
		}finally{
			inputStream.close();
			
			Date endDate = Calendar.getInstance().getTime();
			long diff = endDate.getTime() - startDate.getTime();
			LOG.info("Import finished: " + endDate.toString() + ". Import time: " + Migrate.getDurationBreakdown(diff));
		}	

	}
}
