package nextflow.treevalsummary

import java.nio.file.Path
import java.nio.file.Files
import java.nio.charset.StandardCharsets
import java.io.BufferedWriter
import java.util.concurrent.ConcurrentHashMap

import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.trace.TraceHelper
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskId
import nextflow.processor.TaskProcessor

@Slf4j
@CompileStatic
class TreevalSummaryTraceData implements TraceObserver {

    public static final String DEF_FILE_NAME = "pipeline-trace-${TraceHelper.launchTimestampFmt()}.txt"

    List<String> fields = [
            'task_id',
            'hash',
            'native_id',
            'name',
            'status',
            'exit',
            'submit',
            'duration',
            'realtime',
            '%cpu',
            'peak_rss',
            'peak_vmem',
            'rchar',
            'wchar'
    ]

    List<String> formats
    String separator = '\t'
    boolean overwrite
    private Path tracePath
    private BufferedWriter fileWriter
    @PackageScope Map<TaskId, TraceRecord> current = new ConcurrentHashMap<>()
    private boolean useRawNumber

    void setFields(List<String> entries) {
        def names = TraceRecord.FIELDS.keySet()
        def result = new ArrayList<String>(entries.size())
        for (def item : entries) {
            def thisName = item.trim()
            if (thisName) {
                if (thisName in names)
                    result << thisName
                else {
                    String message = "Not a valid trace field name: '$thisName'"
                    def alternatives = names.bestMatches(thisName)
                    if (alternatives)
                        message += " -- Possible solutions: ${alternatives.join(', ')}"
                    throw new IllegalArgumentException(message)
                }
            }
        }
        this.fields = result
    }

    TreevalSummaryTraceData setFieldsAndFormats(value) {
        List<String> entries
        if (value instanceof String) {
            entries = value.tokenize(', ')
        } else if (value instanceof List) {
            entries = (List) value
        } else {
            throw new IllegalArgumentException("Not a valid trace fields value: $value")
        }

        List<String> fields = new ArrayList<>(entries.size())
        List<String> formats = new ArrayList<>(entries.size())

        for (String x : entries) {
            String name
            String fmt
            int p = x.indexOf(':')
            if (p == -1) {
                name = x
                fmt = TraceRecord.FIELDS.get(name)
            } else {
                name = x.substring(0, p)
                fmt = x.substring(p + 1)
            }

            if (!fmt)
                throw new IllegalArgumentException("Unknown trace field name: `$name`")

            if (useRawNumber && fmt in TraceRecord.NON_PRIMITIVE_TYPES) {
                fmt = 'num'
            }

            fields << name.trim()
            formats << fmt.trim()
        }

        setFields(fields)
        setFormats(formats)

        return this
    }

    TreevalSummaryTraceData useRawNumbers(boolean value) {
        this.useRawNumber = value
        List<String> local = []
        for (String name : fields) {
            def type = TraceRecord.FIELDS.get(name)
            if (useRawNumber && type in TraceRecord.NON_PRIMITIVE_TYPES) {
                type = 'num'
            }
            local << type
        }
        this.formats = local
        return this
    }

    TreevalSummaryTraceData(Path traceFile) {
        this.tracePath = traceFile
    }

    void setFormats(List<String> formats) {
        this.formats = formats
    }

    @Override
    void onFlowCreate(Session session) {
        fileWriter = Files.newBufferedWriter(tracePath, StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND)
        fileWriter.write(fields.join(separator) + '\n')
        fileWriter.flush()
    }

    @Override
    void onFlowComplete() {
        fileWriter?.close()
    }

    @Override
    void onProcessCreate(TaskProcessor process) {}

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        current[trace.taskId] = trace
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        current[trace.taskId] = trace
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        final taskId = handler.task.id
        if (!trace) {
            log.debug "[WARN] Unable to find record for task run with id: ${taskId}"
            return
        }
        current.remove(taskId)
        fileWriter.write(render(trace) + '\n')
        fileWriter.flush()
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        if (trace != null) {
            fileWriter.write(render(trace) + '\n')
            fileWriter.flush()
        }
    }

    String render(TraceRecord trace) {
        assert trace
        trace.renderText(fields, formats, separator)
    }

    @Override
    boolean enableMetrics() {
        return true
    }

    String getTraceData() {
        return "Trace is written directly to file: $tracePath"
    }
}
