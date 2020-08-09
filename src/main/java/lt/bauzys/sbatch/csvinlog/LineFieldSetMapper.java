package lt.bauzys.sbatch.csvinlog;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LineFieldSetMapper implements FieldSetMapper<Line> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

    @Override
    public Line mapFieldSet(FieldSet fieldSet) throws BindException {
        String[] line = fieldSet.getValues();
        return new Line(line[0], LocalDate.parse(line[1], formatter));
    }
}
