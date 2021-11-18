/* Copyright © 2016 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.exigen.eis.jcr;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.AttributesImpl;

public class Export {
    /** must not be too big, as otherwise may get "Protocol violation" exception and similar, seen also NPE at DBConversion._CHARBytesToJavaChars deep inside Oracle driver */
    private static final String DEFAULT_FETCH_SIZE = "30000";

    private static final AttributesImpl EMPTY_ATTRIBUTES = new AttributesImpl();
	private static SimpleDateFormat dates = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat partitionFormat = new SimpleDateFormat("yyyy/MM/dd");
	private String docPrefix = "";
	private String valuePrefix = "";
	private final static Logger LOG = LoggerFactory.getLogger(Export.class);
	private final int fetchSizeProperty = Integer.valueOf(System.getProperty("export.fetchSize", DEFAULT_FETCH_SIZE));

	private ContentHandler th;

	private String schema;
	private int counter;
	private long processedDate;
	private boolean dbStore;
	private String repository =  System.getProperty("repository","default");


	public void init(ContentHandler hd, Connection connection) throws Exception {
		setupDialect();
		this.th = hd;

		schema = getSchemaName(connection);

		dbStore = isDbStore(connection);



	}

	private boolean isDbStore(Connection connection) throws SQLException {
		Statement typeStatement = connection.createStatement();
		try {
			ResultSet typeCountResultSet = typeStatement.executeQuery("select count(*) from " + schema + ".CMCS_STORE where CONFIG like '%local.type=db%'");
			try {

				typeCountResultSet.next();				
				return typeCountResultSet.getInt(1) > 0;				

			}
			finally {
				typeCountResultSet.close();
			}
		}
		finally {
			typeStatement.close();
		}
	}

	private String buildDocumentTypesSql(Connection connection) throws SQLException {
		String i18n = "";
		try {
			i18n = getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENTTYPE", "X_IPB_I18N_%");
			i18n = "A." + i18n + " AS I18N, ";
		}
		catch (IllegalArgumentException e) {
			LOG.warn("Ignoring i18n." + e.getMessage());
		}

		String documentTypesSql = new StringBuilder("select ").append(i18n)
				.append("A.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENTTYPE", "X_IPB_REALNAME_%")).append(" AS NAME, B.NAME as CODE, C.")
				.append(getColumnName(connection, schema, "CM_TYPE_NT_BASE", "X_JCR_UUID_%")).append(" AS UUID, B.NODE_PATH")
				.append(" from dbo_jcr.CM_TYPE_IPB_DOCUMENTTYPE A")
				.append(" inner join dbo_jcr.CM_NODE B on A.NODE_ID=B.ID")
				.append(" inner join dbo_jcr.CM_TYPE_NT_BASE C on B.ID=C.NODE_ID").toString().replace("dbo_jcr", schema);
		LOG.debug("DocumentTypes: " + documentTypesSql);
		return documentTypesSql;
	}

	private String buildFoldersSql(Connection connection) throws SQLException {
		String i18n = "";
		try {
			i18n = getColumnName(connection, schema, "CM_TYPE_IPB_FOLDER", "X_IPB_I18N_%");
			i18n = "A." + i18n + " AS I18N, ";
		}
		catch (IllegalArgumentException e) {
			LOG.warn("Ignoring i18n." + e.getMessage());
		}
		String foldersSql = new StringBuilder("select ").append(i18n)
				.append("B.ID as FOLDER_ID, A.").append(getColumnName(connection, schema, "CM_TYPE_IPB_FOLDER", "X_IPB_PATH_%")).append(" as PATH,  B.NAME as NAME, B.NODE_PATH,")
				.append(" C.").append(getColumnName(connection, schema, "CM_TYPE_NT_BASE", "X_JCR_UUID_%")).append(" as UUID")
				.append(" from dbo_jcr.CM_TYPE_IPB_FOLDER A")
				.append(" inner join dbo_jcr.CM_NODE B on A.NODE_ID=B.ID")
				.append(" inner join dbo_jcr.CM_TYPE_NT_BASE C on B.ID=C.NODE_ID")
				.append("").toString().replace("dbo_jcr", schema);
		LOG.debug("Folders: " + foldersSql);
		return foldersSql;
	}

	private String buildExtDocumentSql(Connection connection) throws SQLException {
		try {
			String extDocumentSql = new StringBuilder("select ").append("B.NODE_ID ")
					.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENTURL", "X_IPB_FOLDER_%")).append(" AS X_IPB_FOLDER")
					.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENTURL", "X_IPB_DOCUMENTTYPE_%")).append(" AS X_IPB_DOCUMENTTYPE")
					.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENTURL", "X_IPB_COMMENT_%")).append(" AS X_IPB_COMMENT")
					.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENTURL", "X_IPB_RESOURCEURL_%")).append(" AS X_IPB_RESOURCEURL")
					.append(", C.NAME as DOCUMENT_NAME")
					.append(", D.NAME as ENTITY_ID")
					.append(", E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_CREATED_%"))
					.append(" as CREATED, E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_CREATEDBY_%"))
					.append(" as CREATED_BY, E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_UPDATED_%"))
					.append(" as UPDATED, E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_UPDATEDBY_%"))
					.append(" as UPDATED_BY")
					.append(", F.").append(getColumnName(connection, schema, "CM_TYPE_NT_BASE", "X_JCR_UUID_%")).append(" AS UUID")
					.append(" from ")
					.append(" dbo_jcr.CM_TYPE_IPB_DOCUMENTURL B ")
					.append(" inner join dbo_jcr.CM_NODE C on B.NODE_ID = C.ID")
					.append(" inner join dbo_jcr.CM_NODE D on C.PARENT_ID=D.ID")
					.append(" inner join dbo_jcr.CM_TYPE_ECR_TRACKABLE E on E.NODE_ID=B.NODE_ID ")
					.append(" inner join dbo_jcr.CM_TYPE_NT_BASE F on B.NODE_ID=F.NODE_ID")
					.toString().replace("dbo_jcr", schema);

			LOG.debug("External Documents: " + extDocumentSql);
			return extDocumentSql;
		}
		catch (IllegalArgumentException e) {
			LOG.warn("Ignoring external documents" + e.getMessage());
			return null;
		}


	}

	private String buildDocumentSql(Connection connection) throws SQLException {
		String documentSql = new StringBuilder("select ").append("a.STORE_CONTENT_ID, a.CONTENT_SIZE,") 
				.append("B.NODE_ID, B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENT", "X_IPB_MIMETYPE_%")).append(" AS X_IPB_MIMETYPE")
				.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENT", "X_IPB_FOLDER_%")).append(" AS X_IPB_FOLDER")
				.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENT", "X_IPB_DOCUMENTTYPE_%")).append(" AS X_IPB_DOCUMENTTYPE")
				.append(", B.").append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENT", "X_IPB_COMMENT_%")).append(" AS X_IPB_COMMENT")
				.append(", C.NAME as DOCUMENT_NAME")
				.append(", D.NAME as ENTITY_ID")
				.append(", E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_CREATED_%"))
				.append(" as CREATED, E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_CREATEDBY_%"))
				.append(" as CREATED_BY, E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_UPDATED_%"))
				.append(" as UPDATED, E.").append(getColumnName(connection, schema, "CM_TYPE_ECR_TRACKABLE", "X_ECR_UPDATEDBY_%")).append(" as UPDATED_BY")
				.append(", F.").append(getColumnName(connection, schema, "CM_TYPE_NT_BASE", "X_JCR_UUID_%")).append(" AS UUID")
				.append(" from dbo_jcr.CMCS_CONTENT a")
				.append(" inner join dbo_jcr.CM_TYPE_IPB_DOCUMENT B on ").append(docPrefix).append("a.ID = B.")
				.append(getColumnName(connection, schema, "CM_TYPE_IPB_DOCUMENT", "X_IPB_CONTENT_%"))
				.append(" inner join dbo_jcr.CM_NODE C on B.NODE_ID = C.ID")
				.append(" inner join dbo_jcr.CM_NODE D on C.PARENT_ID=D.ID")
				.append(" inner join dbo_jcr.CM_TYPE_ECR_TRACKABLE E on E.NODE_ID=B.NODE_ID ")
                .append(" inner join dbo_jcr.CM_TYPE_NT_BASE F on B.NODE_ID=F.NODE_ID")
				.toString().replace("dbo_jcr", schema);

		LOG.debug("Documents: " + documentSql);
		return documentSql;
	}


	private static void setupDriver() throws Exception {
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

	private void setupDialect() throws Exception {
		if (System.getProperty("dbUrl").contains("oracle")) {
			docPrefix = "'.'||";
			valuePrefix = ".";
		}
	}

	private static TransformerHandler setupHandler(OutputStream fos)throws Exception {
		SAXTransformerFactory tf = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
		TransformerHandler hd = tf.newTransformerHandler();
		Transformer serializer = hd.getTransformer();
		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT, "yes");				
		hd.setResult(new StreamResult(fos));
		return hd;
	}



	public void run(ContentHandler hd, Connection connection) throws Exception {

		init(hd, connection);

		th.startDocument();
		startElement("efolder");

		startElement("documentTypes");
		writeDocumentTypes(connection);
		endElement("documentTypes");

		startElement("folders");
		writeFolders(connection);
		endElement("folders");

		startElement("documents");
		writeDocuments(connection);
		endElement("documents");

		String extDocumentSql = buildExtDocumentSql(connection);
		if (extDocumentSql != null) {
			startElement("extDocuments");
			writeExtDocuments(connection, extDocumentSql);
			endElement("extDocuments");
		}

		endElement("efolder");
		th.endDocument();

	}

	private void writeFolders(Connection connection) throws Exception {
		Statement statement = connection.createStatement();
		try {
			ResultSet resultSet = statement.executeQuery(buildFoldersSql(connection));
			try {
				while (resultSet.next()) {
					writeFolder(resultSet);
				}
			}
			finally {
				resultSet.close();
			}
		}
		finally {
			statement.close();
		}

	}

	private void writeFolder(ResultSet resultSet) throws Exception {
		startElement("folder");

		writeElement("id", parseJCRString(resultSet.getString("UUID")));

		String name = parseJCRString(resultSet.getString("NAME"));
		if (name.equals("root")) {
			name = "";
		}
		writeElement("name", name);

		writeElement("path", parseEntityType(parseJCRString(resultSet.getString("NODE_PATH"))) + "/"+ parseJCRString(resultSet.getString("PATH")));
		if (resultSet.getMetaData().getColumnCount() > 5) {
			writeLabels(parseJCRString(resultSet.getString("I18N")));
		}

		Long folderId = resultSet.getLong("FOLDER_ID");
		writeDocumentTypeIds(resultSet.getStatement().getConnection(), folderId);
		writePrivileges(resultSet.getStatement().getConnection(), folderId, "writePrivilege");
		writePrivileges(resultSet.getStatement().getConnection(), folderId, "readPrivilege");
		endElement("folder");

	}

	private void writePrivileges(Connection connection, Long folderId, String privilegeName) throws Exception {
		String sql = new String("select A.NAME as NAME, B.STRING_VALUE, B.NODE_ID from dbo_jcr.CM_NODE_UNSTRUCTURED A" +
				" inner join dbo_jcr.CM_NODE_UNSTRUCT_VALUES B on A.ID=B.PROPERTY_ID" +
				" where A.NAME = ? and B.NODE_ID = ?").replace("dbo_jcr", schema);

		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		try {
			preparedStatement.setString(1, valuePrefix + privilegeName);
			preparedStatement.setLong(2, folderId);
			ResultSet resultSet = preparedStatement.executeQuery();
			try {
				startElement(privilegeName+"s");
				while (resultSet.next()) {
					String privilege =parseJCRString(resultSet.getString("STRING_VALUE")); 
					if (privilege.length() > 0) {
						writeElement(privilegeName, privilege);
					}
				}
				endElement(privilegeName+"s");
			}
			finally {
				resultSet.close();
			}
		}
		finally {
			preparedStatement.close();
		}
	}

	private void writeDocumentTypeIds(Connection connection, Long folderId) throws Exception {
		String sql = new String("select A.NAME as NAME, B.STRING_VALUE, B.NODE_ID from dbo_jcr.CM_NODE_UNSTRUCTURED A" +
				" inner join dbo_jcr.CM_NODE_UNSTRUCT_VALUES B on A.ID=B.PROPERTY_ID" +
				" where A.NAME = ? and B.NODE_ID = ?").replace("dbo_jcr", schema);
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		try {
			preparedStatement.setString(1, valuePrefix + "allowedTypes");
			preparedStatement.setLong(2, folderId);
			ResultSet resultSet = preparedStatement.executeQuery();
			try {
				startElement("allowedTypes");
				while (resultSet.next()) {
					writeElement("allowedType", parseJCRString(resultSet.getString("STRING_VALUE")));
				}
				endElement("allowedTypes");
			}
			finally {
				resultSet.close();
			}

		}
		finally {
			preparedStatement.close();
		}

	}

	private void writeDocumentTypes(Connection connection) throws Exception {
		Statement statement = connection.createStatement();
		try {
			ResultSet resultSet = statement.executeQuery(buildDocumentTypesSql(connection));
			try {
				while (resultSet.next()) {
					writeDocumentType(resultSet);
				}
			}
			finally {
				resultSet.close();
			}
		}
		finally {
			statement.close();
		}
	}

	private void writeDocumentType(ResultSet resultSet) throws Exception {
		startElement("documentType");
		writeElement("name", parseJCRString(resultSet.getString("NAME")));
		writeElement("code", parseJCRString(resultSet.getString("CODE")));
		writeElement("uuid", parseJCRString(resultSet.getString("UUID")));
		writeElement("entityType", parseEntityType(parseJCRString(resultSet.getString("NODE_PATH"))));
		if (resultSet.getMetaData().getColumnCount() > 4) {
			writeLabels(parseJCRString(resultSet.getString("I18N")));
		}
		endElement("documentType");
	}

	private void startElement(String name) throws Exception {
		th.startElement("", "", name, EMPTY_ATTRIBUTES);
	}

	private void endElement(String name) throws Exception {
		th.endElement("", "", name);
	}



	private void writeDocuments(Connection connection) throws Exception {
		Statement documentStatement = connection.createStatement();
		try {
			documentStatement.setFetchSize(fetchSizeProperty);
			ResultSet documentResultSet = documentStatement.executeQuery(buildDocumentSql(connection));
			try {
				while (documentResultSet.next()) {
					writeDocument(documentResultSet);
				}
			}
			finally {
				documentResultSet.close();
			}
		}
		finally {
			documentStatement.close();
		}
	}

	private void writeExtDocuments(Connection connection, String extDocumentSql) throws Exception {
		Statement documentStatement = connection.createStatement();
		try {
			documentStatement.setFetchSize(fetchSizeProperty);
			ResultSet documentResultSet = documentStatement.executeQuery(extDocumentSql);
			try {
				while (documentResultSet.next()) {
					writeExtDocument(documentResultSet);
				}
			}
			finally {
				documentResultSet.close();
			}
		}
		finally {
			documentStatement.close();
		}
	}

	private void writeDocument(ResultSet resultSet) throws Exception {
		counter++;
		if (processedDate == 0) {
			processedDate = System.currentTimeMillis();
		}
		if (counter % 10000 == 0) {
			long currentDate = System.currentTimeMillis();
			long diff = currentDate - processedDate;
			processedDate = currentDate;
			LOG.info("Processed documents: " + counter + " (10000 in " + Migrate.getDurationBreakdown(diff) + ")");
		}



		String contentId = getContentId(resultSet);
		if(dbStore){
			exportFile(resultSet.getStatement().getConnection(),contentId);
		}

		startElement("document");		
        writeElement("uuid", parseJCRString(resultSet.getString("UUID")));
		writeElement("contentId", contentId );
		writeElement("contentSize", parseJCRString(resultSet.getString("CONTENT_SIZE")));
		writeElement("mimeType", parseJCRString(resultSet.getString("X_IPB_MIMETYPE")));
		writeElement("folderId", parseJCRString(resultSet.getString("X_IPB_FOLDER")));
		writeElement("documentTypeCode", parseJCRString(resultSet.getString("X_IPB_DOCUMENTTYPE")));
		writeElement("name", parseJCRString(resultSet.getString("DOCUMENT_NAME")));
		writeElement("comment", parseJCRString(resultSet.getString("X_IPB_COMMENT")));
		writeElement("entityId", parseJCRString(resultSet.getString("ENTITY_ID")));
		writeElement("createdDate", dates.format(resultSet.getTimestamp("CREATED")));
		writeElement("createdBy", parseJCRString(resultSet.getString("CREATED_BY")));
		writeElement("modifiedDate", dates.format(resultSet.getTimestamp("UPDATED")));
		writeElement("modifiedBy", parseJCRString(resultSet.getString("UPDATED_BY")));
		endElement("document");



	}

	private void exportFile(Connection connection, String nodeId) throws SQLException, IOException {
		
		String partition = nodeId.substring(0,nodeId.lastIndexOf('/'));
		String id = nodeId.substring(nodeId.lastIndexOf('/') + 1);
		String sql = "select DATA from CM_STORE_DEFAULT where ID = ? ";
		PreparedStatement stmt = connection.prepareStatement(sql);
		try{
			stmt.setLong(1, Long.parseLong(id));
			ResultSet rs = stmt.executeQuery();
			if(rs.next()){
				String root = System.getProperty("cmisFileRepositoryHome",System.getProperty("user.home"));
				File file = new File( root + File.separator + repository + File.separator + partition );
				file.mkdirs();
				FileOutputStream out = new FileOutputStream(new File(file, id));
				try{
					InputStream in = rs.getBinaryStream(1);
					try{
					byte buf[] = new byte[1024*8];
					int len;
					while((len = in.read(buf)) > 0){
						out.write(buf, 0, len);
					}
					}finally{
						in.close();
					}
				}finally{
					out.close();
				}

			}

		}finally{
			stmt.close();
		}


	}

	private String getContentId(ResultSet resultSet) throws SQLException {
		if(dbStore){
			return partitionFormat.format(resultSet.getTimestamp("UPDATED")) + "/" + parseJCRString(resultSet.getString("STORE_CONTENT_ID"));
		}
		return parseJCRString(resultSet.getString("STORE_CONTENT_ID"));
	}

	private void writeExtDocument(ResultSet resultSet) throws Exception {
		counter++;

		if (counter % 10000 == 0) {
			LOG.info("Processed documents: " + counter);
		}

		startElement("extDocument");
		writeElement("uuid", parseJCRString(resultSet.getString("UUID")));
		writeElement("folderId", parseJCRString(resultSet.getString("X_IPB_FOLDER")));
		writeElement("documentTypeCode", parseJCRString(resultSet.getString("X_IPB_DOCUMENTTYPE")));
		writeElement("comment", parseJCRString(resultSet.getString("X_IPB_COMMENT")));
		writeElement("resourceUrl", parseJCRString(resultSet.getString("X_IPB_RESOURCEURL")));
		writeElement("name", parseJCRString(resultSet.getString("DOCUMENT_NAME")));
		writeElement("entityId", parseJCRString(resultSet.getString("ENTITY_ID")));
		writeElement("createdDate", dates.format(resultSet.getTimestamp("CREATED")));
		writeElement("createdBy", parseJCRString(resultSet.getString("CREATED_BY")));
		writeElement("modifiedDate", dates.format(resultSet.getTimestamp("UPDATED")));
		writeElement("modifiedBy", parseJCRString(resultSet.getString("UPDATED_BY")));
		endElement("extDocument");
	}

	private void writeLabels(String i18nInfo) throws Exception {
		if (i18nInfo == null) {
			return;
		}
		startElement("labels");
		for( String label : i18nInfo.trim().split("\\r?\\n")){
			String [] record = label.split("=");
			if(record.length == 2){
				startElement("label");
				writeElement("locale", record[0]);
				writeElement("text", record[1]);
				endElement("label");
			}
		}
		endElement("labels");
	}

	private void writeElement(String name, Object value) throws Exception {
		startElement(name);
		writeText(value);
		endElement(name);
	}

	private void writeText(Object object) throws Exception {
		if (object == null) {
			return;
		}
		String text = object.toString();
		th.characters(text.toCharArray(), 0, text.length());
	}

	public static String extractContent(Connection connection, String schema, Long nodeId, String columnName) throws SQLException {
		return extractNode(connection, nodeId, columnName, schema);
	}

	public static String getColumnName(Connection connection, String schema, String tableNamePattern, String columnNamePattern) throws SQLException {

		ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), schema, tableNamePattern, columnNamePattern);
		try {
			String columnName = null;
			while (rs.next()) {
				String currentName = rs.getString("COLUMN_NAME"); 
				if (columnName == null || (currentName.length() < columnName.length())) {
					columnName = currentName;
				}
			}
			if (columnName == null) {
				throw new IllegalArgumentException("Column not found " + schema + " " + tableNamePattern + " " + columnNamePattern);
			}
			return columnName;
		}
		finally {
			rs.close();
		}
	}

	private static String extractNode(Connection connection, Long nodeId, String columnName, String schema) throws SQLException {
		PreparedStatement statement = connection.prepareStatement("select " + columnName + " from " + schema + ".CM_TYPE_IPB_DOCUMENT where NODE_ID = ?");
		try {
			statement.setLong(1, nodeId);
			ResultSet document = statement.executeQuery();
			try {
				if (document.next()) {
					String contentId = document.getString(1);
					return extractContent(connection, nodeId, schema, contentId);
				}
				else {
					throw new IllegalArgumentException("Node " + nodeId + " not found.");
				}
			}
			finally {
				document.close();
			}
		}
		finally {
			statement.close();
		}
	}

	private static String extractContent(Connection connection, Long nodeId, String schema, String contentId) throws SQLException {
		PreparedStatement contentStatement = connection.prepareStatement("select STORE_CONTENT_ID from " + schema + ".CMCS_CONTENT where ID = ?");
		try {
			contentStatement.setLong(1, Long.parseLong(parseJCRString(contentId)));
			ResultSet contentResultSet = contentStatement.executeQuery();
			if (contentResultSet.next()) {
				return parseJCRString(contentResultSet.getString(1));
			}
			else {
				throw new IllegalArgumentException("Content not found " + contentId + " for node " + nodeId);
			}
		}
		finally {
			contentStatement.close();
		}
	}

	private static String parseJCRString(String value) {
		if (value == null) {
			return "";
		}
		if (value.startsWith(".")) {
			return value.substring(1);
		}
		else {
			return value;
		}
	}

	public static String getSchemaName(Connection connection) throws SQLException{
		if (connection.getMetaData().getDatabaseProductName().toLowerCase().contains("oracle")) {
			return connection.getMetaData().getUserName();
		}

		Statement st = connection.createStatement();
		try {
			st.execute("select name from sys.schemas where name like '%|_jcr' escape '|'");
			ResultSet rs = st.getResultSet();
			try {
				if(rs.next()){
					String result = rs.getString("name");
					return result;
				}else {
					throw new SQLException("Unable to find JCR schema by %_jcr pattern");
				}
			}
			finally {
				rs.close();
			}
		} 
		finally {
			st.close();

		}
	}

	public static String parseEntityType(String value) {
		Matcher matcher = Pattern.compile("/\\d+\\:efolder\\{\\d\\}/([^\\{]+)\\{.+").matcher(value);
		if (matcher.matches()) {
			return matcher.group(1);
		}
		else {
			return "";
		}


	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String fileName = System.getProperty("migration.file", "jcr-cmis-migration.xml.gz");
		Date startDate = Calendar.getInstance().getTime();
		LOG.info("Export started: " + startDate.toString());
		LOG.info("Exporting to file: " + fileName);
		OutputStream fos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
		try {
			setupDriver();
			TransformerHandler hd = setupHandler(fos);			
			Export export = new Export();
			String dbUrl = System.getProperty("dbUrl");
			if (dbUrl.startsWith("jdbc:sqlserver") && !dbUrl.contains("selectMethod")) {
				if (!dbUrl.endsWith(";")) {
					dbUrl = dbUrl + ";";
				}
				dbUrl = dbUrl + "selectMethod=cursor";
			}
			Connection connection = DriverManager.getConnection(dbUrl,
					System.getProperty("dbLogin"),
					System.getProperty("dbPassword"));
			try {
				connection.setAutoCommit(false);
				export.run(hd,connection);
				Date endDate = Calendar.getInstance().getTime();
				long diff = endDate.getTime() - startDate.getTime();
				LOG.info("Export finished: " + endDate.toString() + ". Export time: " + Migrate.getDurationBreakdown(diff));
			}
			finally {
				connection.close();
			}
		}
		finally {
			fos.close();
		}
	}
}
