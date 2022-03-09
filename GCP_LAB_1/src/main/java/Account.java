import org.apache.beam.sdk.schemas.JavaFieldSchema;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
@VisibleForTesting
@DefaultSchema(JavaFieldSchema.class)
public class Account {
    int id;
    String name;
    String surname;

    public Account(int id, String name, String surname) {
        this.id = id;
        this.name = name;
        this.surname = surname;
    }
}
