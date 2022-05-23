public class YugabyteDBWrongIncludeListTest {
    // These comments are here only for the time being and the listed steps need to be performed
    // to test the functionality

    /*
     * 1. Create a table test
     * 2. Create stream ID using yb-admin, let's call it STREAM_ID
     * 3. Now create another table test2, this won't be a part of STREAM_ID
     * 4. Now deploy the connector with STREAM_ID and include test and test2 in table.include.list
     * 5. The connector will throw a warning that table test2 is not a part of STREAM_ID
     * 6. Delete the connector and deploy it again after adding "ignore.exceptions":"false"
     * 7. This time the connector will just log a warning and move ahead with processing the changes
     * for the table test.
     */
}
