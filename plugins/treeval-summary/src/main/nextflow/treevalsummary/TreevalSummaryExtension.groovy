package nextflow.treevalsummary

import java.nio.file.Path
import nextflow.treevalsummary.TreevalSummaryTraceData
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.script.WorkflowMetadata
import nextflow.script.ScriptBinding.ParamsMap
import java.nio.file.Paths


import java.time.OffsetDateTime
import java.time.Duration

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/**
 * A plugin extension point for the TreevalSummary plugin
 * @author : sainsachiko
 */

@Slf4j
@CompileStatic
class TreevalSummaryExtension extends PluginExtensionPoint {

    private Session session

    @Override
    protected void init(Session session) {
        this.session = session
    }

    @Function 
    String version(WorkflowMetadata workflow) {
        String version_string = ""

        if (workflow.manifest.version) {
            def prefix_v = workflow.manifest.version[0] != 'v' ? 'v' : ''
            version_string += "${prefix_v}${workflow.manifest.version}"
        }

        if (workflow.commitId) {
            def git_shortsha = workflow.commitId.substring(0, 7)
            version_string += "-g${git_shortsha}"
        }

        return version_string
    }

    @Function
    void summary(WorkflowMetadata workflow, ParamsMap params, LinkedHashMap metrics, String dbPath) {

        def date_completed = OffsetDateTime.now()

        def input_data = [:]
        input_data['version']           = version( workflow )
        input_data['runName']           = workflow.runName
        input_data['session_id']        = workflow.sessionId
        input_data['duration']          = Duration.between( workflow.start, date_completed ).toSeconds()
        input_data['DateStarted']       = workflow.start
        input_data['DateCompleted']     = date_completed
        input_data['entry']             = params.entry

        input_data['input_yaml']        = params.input
        input_data['sample_name']       = metrics.sample_id
        input_data['rf_data']           = metrics.rf_data
        input_data['pb_data']           = metrics.pb_data
        input_data['cm_data']           = metrics.cm_data

        def output_directory = new File("${params.tracedir}/")
        if (!output_directory.exists()) {
            output_directory.mkdirs()
        }

        def output_hf = new File( output_directory, "pipeline_rundata_${input_data.sample_name}_${input_data.entry}_${params.trace_timestamp}.txt" )
        output_hf.write """\
                        ---RUN_DATA---
                        Pipeline_version:   ${input_data.version}
                        Pipeline_runname:   ${input_data.runName}
                        Pipeline_session:   ${input_data.session_id}
                        Pipeline_duration:  ${input_data.duration}
                        Pipeline_datestart:  ${input_data.DateStarted}
                        Pipeline_datecomp:  ${input_data.DateCompleted}
                        Pipeline_entrypnt:  ${input_data.entry}
                        ---INPUT_DATA---
                        InputSampleID:      ${input_data.sample_name}
                        InputYamlFile:      ${input_data.input_yaml}
                        InputAssemblyData:  ${input_data.rf_data}
                        Input_PacBio_Files: ${input_data.pb_data}
                        Input_Cram_Files:   ${input_data.cm_data}
                        ---RESOURCES---
                        """.stripIndent()

        def treevalFile = session.getBaseDir().resolve(TreevalSummaryTraceData.DEF_FILE_NAME)

        if (treevalFile.exists()) {
            output_hf.append(treevalFile.text)

            if (dbPath) {
            File dbFile = new File(dbPath)
                if (!dbFile.exists()) {
                    log.info "DuckDB does not exist, creating new DB at: $dbPath"
                    createDB(dbPath)
                }
                insertDB(dbPath, input_data, treevalFile.toFile())
            } else {
                log.warn "No DuckDB path provided — skipping DB operations"
            }
            java.nio.file.Files.delete(treevalFile)

        } else {
            log.warn "pipeline trace file not found: ${treevalFile}"
        }
    }

    @Function
    void createDB(String dbPath) {
        Class.forName("org.duckdb.DuckDBDriver")
        def conn = DriverManager.getConnection("jdbc:duckdb:${dbPath}")
        try {
            def stmt = conn.createStatement()

            // Create Meta Table if it doesn't exist
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    Pipeline_version TEXT,
                    Pipeline_runname TEXT,
                    Pipeline_session TEXT,
                    Pipeline_duration TEXT,
                    Pipeline_datestart TEXT,
                    Pipeline_datecomp TEXT,
                    Pipeline_entrypnt TEXT,
                    InputSampleID TEXT,
                    InputYamlFile TEXT,
                    InputAssemblyData TEXT,
                    Input_PacBio_Files TEXT,
                    Input_Cram_Files TEXT
                )
            """)

            // Create Trace Table if it doesn't exist
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS trace (
                    Pipeline_runname TEXT,
                    Pipeline_session TEXT,
                    task_id TEXT,
                    hash TEXT,
                    native_id TEXT,
                    name TEXT,
                    status TEXT,
                    exit TEXT,
                    submit TEXT,
                    duration TEXT,
                    realtime TEXT,
                    cpu_percent TEXT,
                    peak_rss TEXT,
                    peak_vmem TEXT,
                    rchar TEXT,
                    wchar TEXT
                )
            """)

            stmt.close()
            log.info("DuckDB tables 'meta' and 'trace' created or already exist at ${dbPath}")

        } catch (Exception e) {
            log.error("Failed to create DuckDB at ${dbPath}: ${e.message}", e)
        } finally {
            conn.close()
        }
    }

    @Function
    void insertDB(String dbPath, Map input_data, File treevalFile) {
        Class.forName("org.duckdb.DuckDBDriver")
        def conn = DriverManager.getConnection("jdbc:duckdb:${dbPath}")
        try {
            // === Insert into meta ===
            def metaStmt = conn.prepareStatement("""
                INSERT INTO meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            metaStmt.setString(1, input_data.version?.toString())
            metaStmt.setString(2, input_data.runName?.toString())
            metaStmt.setString(3, input_data.session_id?.toString())
            metaStmt.setString(4, input_data.duration?.toString())
            metaStmt.setString(5, input_data.DateStarted?.toString())
            metaStmt.setString(6, input_data.DateCompleted?.toString())
            metaStmt.setString(7, input_data.entry?.toString())
            metaStmt.setString(8, input_data.sample_name?.toString())
            metaStmt.setString(9, input_data.input_yaml?.toString())
            metaStmt.setString(10, input_data.rf_data?.toString())
            metaStmt.setString(11, input_data.pb_data?.toString())
            metaStmt.setString(12, input_data.cm_data?.toString())
            metaStmt.executeUpdate()
            metaStmt.close()

            // === Insert into trace ===
            if (treevalFile.exists() && treevalFile.length() > 0) {
                log.info("Reading trace file: ${treevalFile}")
                def lines = treevalFile.readLines()
                if (lines.size() < 2) {
                    log.warn("Trace file has no data rows: ${treevalFile}")
                } else {
                    def header = lines[0].split('\t')
                    def rows = lines[1..-1]  // skip header

                    def traceStmt = conn.prepareStatement("""
                        INSERT INTO trace VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """)

                    for (line in rows) {
                        try {
                            def cols = line.split('\t', -1)  // preserve empty strings
                            def colMap = [:]
                            header.eachWithIndex { key, idx -> colMap[key.trim()] = cols[idx]?.trim() }
                            traceStmt.setString(1, input_data.runName?.toString())
                            traceStmt.setString(1, input_data.session_id?.toString())
                            traceStmt.setString(2, colMap['task_id']?.toString())
                            traceStmt.setString(3, colMap['hash']?.toString())
                            traceStmt.setString(4, colMap['native_id']?.toString())
                            traceStmt.setString(5, colMap['name']?.toString())
                            traceStmt.setString(6, colMap['status']?.toString())
                            traceStmt.setString(7, colMap['exit']?.toString())
                            traceStmt.setString(8, colMap['submit']?.toString())
                            traceStmt.setString(9, colMap['duration']?.toString())
                            traceStmt.setString(10, colMap['realtime']?.toString())
                            traceStmt.setString(11, colMap['%cpu']?.toString())  // or 'cpu_percent' if header renamed
                            traceStmt.setString(12, colMap['peak_rss']?.toString())
                            traceStmt.setString(13, colMap['peak_vmem']?.toString())
                            traceStmt.setString(14, colMap['rchar']?.toString())
                            traceStmt.setString(15, colMap['wchar']?.toString())
                            traceStmt.addBatch()
                        } catch (Exception e) {
                            log.warn("Skipping bad row in trace: ${line}\nReason: ${e.message}")
                        }
                    }

                    traceStmt.executeBatch()
                    traceStmt.close()
                    log.info("Inserted ${rows.size()} trace rows into DuckDB at ${dbPath}")
                }
            } else {
                log.warn "Trace file missing or empty: ${treevalFile}"
            }

            log.info("Inserted log data into DuckDB at ${dbPath}")
        } finally {
            conn.close()
        }
    }



}

