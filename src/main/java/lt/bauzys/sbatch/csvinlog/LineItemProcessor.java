package lt.bauzys.sbatch.csvinlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class LineItemProcessor implements ItemProcessor<Line, Line>, StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(LineItemProcessor.class);

    @Override
    public Line process(final Line line) throws Exception {
        final String name = line.getName();
        final LocalDate dob = line.getDob();
        final Line newLine = new Line(name, dob);

        Long age = ChronoUnit.YEARS.between(line.getDob(), LocalDate.now());
        log.debug("Calculated age " + age + " for line " + line.toString());
        newLine.setAge(age);
        return line;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.debug("Line Processor initialized.");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.debug("Line Processor ended.");
        return ExitStatus.COMPLETED;
    }
}
