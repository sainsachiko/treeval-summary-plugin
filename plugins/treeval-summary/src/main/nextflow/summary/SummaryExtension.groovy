package nextflow.Summary

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

import java.time.OffsetDateTime
import java.time.Duration

/**
 * Example showing how to implement treeval-summary function
 */
class SummaryExtension extends PluginExtensionPoint {

    @Override
    void init(Session session) {}
    
    @Function
    void TreevalSummary(Session session, params, Map metrics) {
        def date_completed = OffsetDateTime.now()

        def input_data = [:]
        input_data['version']           = NfcoreTemplate.version( workflow )
        input_data['runName']           = session.runName
        input_data['session_id']        = session.sessionId
        input_data['duration']          = Duration.between( session.start, date_completed ).toSeconds()
        input_data['DateStarted']       = session.start
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

        def output_hf = new File( output_directory, "input_data_${input_data.sample_name}_${input_data.entry}_${params.trace_timestamp}.txt" )
        output_hf.write """\
                        ---RUN_DATA---
                        Pipeline_version:   ${input_data.version}
                        Pipeline_runname:   ${input_data.runName}
                        Pipeline_session:   ${input_data.session_id}
                        Pipeline_duration:  ${input_data.duration}
                        Pipeline_datastrt:  ${input_data.DateStarted}
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

        def full_file = new File( output_directory, "TreeVal_run_${input_data.sample_name}_${input_data.entry}_${params.trace_timestamp}.txt" )
        def file_locs = ["${params.tracedir}/input_data_${input_data.sample_name}_${input_data.entry}_${params.trace_timestamp}.txt",
                            "${params.tracedir}/pipeline_execution_${params.trace_timestamp}.txt"]
        file_locs.each{ full_file.append( new File( it ).getText() ) }

    }
}
