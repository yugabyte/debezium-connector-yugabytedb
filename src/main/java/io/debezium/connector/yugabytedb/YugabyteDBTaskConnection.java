package io.debezium.connector.yugabytedb;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import org.postgresql.PGNotification;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.*;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.jdbc.*;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.util.LruCache;
import org.postgresql.util.PGobject;
import org.postgresql.xml.PGXmlFactoryFactory;

final class YugabyteDBTaskConnection implements BaseConnection {
    private final Encoding encoding;
    private final TypeInfo typeInfo = new TypeInfoCache(this, -1);

    YugabyteDBTaskConnection(Encoding encoding) {
        this.encoding = encoding;
    }

    /**
     * {@inheritDoc}
     */
    public Encoding getEncoding() throws SQLException {
        return encoding;
    }

    /**
     * {@inheritDoc}
     */
    public TypeInfo getTypeInfo() {
        return typeInfo;
    }

    /**
     * {@inheritDoc}
     */
    public void cancelQuery() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public ResultSet execSQLQuery(String s) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public ResultSet execSQLQuery(String s, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void execSQLUpdate(String s) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public QueryExecutor getQueryExecutor() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public ReplicationProtocol getReplicationProtocol() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(String type, String value, byte[] byteValue) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean haveMinimumServerVersion(int ver) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean haveMinimumServerVersion(Version ver) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public byte[] encodeString(String str) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String escapeString(String str) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean getStandardConformingStrings() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public TimestampUtils getTimestampUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Logger getLogger() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean getStringVarcharFlag() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public TransactionState getTransactionState() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean binaryTransferSend(int oid) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isColumnSanitiserDisabled() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void addTimerTask(TimerTask timerTask, long milliSeconds) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void purgeTimerTasks() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public LruCache<FieldMetadata.Key, FieldMetadata> getFieldMetadataCache() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public CachedQuery createQuery(String sql, boolean escapeProcessing, boolean isParameterized, String... columnNames)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Statement createStatement() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean getAutoCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public java.sql.Array createArrayOf(String typeName, Object elements) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PGNotification[] getNotifications() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PGNotification[] getNotifications(int timeoutMillis) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public CopyManager getCopyAPI() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public LargeObjectManager getLargeObjectAPI() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public Fastpath getFastpathAPI() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void addDataType(String type, String className) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void addDataType(String type, Class<? extends PGobject> klass) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setPrepareThreshold(int threshold) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public int getPrepareThreshold() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setDefaultFetchSize(int fetchSize) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public int getDefaultFetchSize() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public int getBackendPID() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String escapeIdentifier(String identifier) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public String escapeLiteral(String literal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PreferQueryMode getPreferQueryMode() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public AutoSave getAutosave() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void setAutosave(AutoSave autoSave) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public PGReplicationConnection getReplicationAPI() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getParameterStatuses() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getParameterStatus(String parameterName) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PGXmlFactoryFactory getXmlFactoryFactory() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hintReadOnly() {
        return false;
    }

    @Override
    public boolean getLogServerErrorDetail() {
        return false;
    }

    @Override
    public void setAdaptiveFetch(boolean adaptiveFetch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getAdaptiveFetch() {
        throw new UnsupportedOperationException();
    }
}
