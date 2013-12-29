<?php

/*
.---------------------------------------------------------------------------.
|  Software: RETSHELPER - PHP Class to interface RETS with Database         |
|   Version: 1.0                                                            |
|   Contact: corey@coreyrowell.com                                          |
|      Info: None                                                           |
|   Support: corey@coreyrowell.com                                          |
| ------------------------------------------------------------------------- |
|    Author: Corey Rowell - corey@coreyrowell.com                           |
| Copyright (c) 2013, Corey Rowell. All Rights Reserved.                    |
| ------------------------------------------------------------------------- |
|   License: This content is released under the                             |
|                   (http://opensource.org/licenses/MIT) MIT License.       |                                    |
'---------------------------------------------------------------------------'
*/

/*
.---------------------------------------------------------------------------.
|   This software requires the use of the PHPRETS library                   |
|   http://troda.com/projects/phrets/                                       |
'---------------------------------------------------------------------------'
*/
define("BASE_PATH", $_SERVER['DOCUMENT_ROOT']."basedir/");

class RETSHELPER
{

    // Defaults
    private $rets, $auth, $config, $database, $mysqli, $data, $log, $scriptstart, $scriptend,
            $previous_start_time, $current_start_time, $updates_log, $active_ListingRids = array();

    public function __construct()
    {

        // Require PHRETS library
        require_once("phrets.php");
        // Start rets connection
        $this->rets = new phRETS;
        $this->scriptstart = date("m-d-y_h-i-s", time());

        // RETS Server Info
        $this->auth['url']          = 'http://www.dis.com:6103/rets/login';//MLS_URL;
        $this->auth['username']     = 'Joe';        //MLS_USERNAME;
        $this->auth['password']     = 'Schmoe';     //MLS_PASS;
        $this->auth['retsversion']  = '';           //USER Agent Version
        $this->auth['useragent']    = '';           //USER Agent

        // RETS Options
        $this->config['property_classes']   = array("COM", "RES");
        $this->config['KeyField']           = "ListingID";
        $this->config['offset_support']     = FALSE; // Enable if RETS server supports 'offset'
        $this->config['useragent_support']  = FALSE;
        $this->config['images_path']        = BASE_PATH."listing_photos/";
        $this->config['logs_path']          = BASE_PATH."logs/";
        $this->config['start_times_path']   = BASE_PATH."logs/";
        $this->config['previous_start_time'] = $this->get_previous_start_time();
        $this->config['create_tables']      = FALSE; // Create tables for classes (terminates program)

        // Log to screen?
        $this->config['to_screen']          = TRUE;

        // Database Config
        $this->database['host']     = 'localhost';  //DB_SERVER;
        $this->database['username'] = 'root';       //DB_USER;
        $this->database['password'] = '';           //DB_PASS;
        $this->database['database'] = 'test';       //DB_NAME;

        // Set PHP memory limit higher
        ini_set('memory_limit', '128M');
        set_time_limit(0);

        // Load any config processes
        $this->config_init();

        // Load the run function
        $this->run();
    }

    private function config_init()
    {
        // Set offset support based on config
        if($this->config['offset_support'])
        {
            $this->rets->SetParam("offset_support", true);
        } else {
            $this->rets->SetParam("offset_support", false);
        }

        if($this->config['useragent_support'])
        {
            $this->rets->AddHeader("RETS-Version", $this->auth['retsversion']);
            $this->rets->AddHeader("User-Agent", $this->auth['useragent']);
        }
    }

    public function run()
    {
        // Start Logging
        $this->logging_start();

        // RETS Connection
        $this->connect();

        // Connect to Database
        $this->database_connect();

        if($this->config['create_tables'])
        {
            $this->log_data("Creating database tables, program will exit after finishing.");
            foreach ($this->config['property_classes'] as $class)
            {
                $this->log_data("Creating table for: " . $class);
                $this->create_table_for_property_class($class);
            }
            $this->log_data("Exiting program.");
            return;
        }

        // Get Properties (and images)
        $this->get_properties_by_class();

        // Close RETS Connection
        $this->disconnect();

        // Delete inactive photos
//        $this->file_delete_photos();

        // Delete inactive listings
        $this->database_delete_records();

        // Insert new listings
        $this->database_insert_records();

        // Disconnect from Database
        $this->database_disconnect();

        // End Logging
        $this->logging_end();

        // Time for next scheduled update
        $this->set_previous_start_time();
    }


    private function connect()
    {
        $this->log_data("Connecting to RETS...");
        // Connect to RETS
        $connect = $this->rets->Connect($this->auth['url'], $this->auth['username'], $this->auth['password']);

        if($connect)
        {
            $this->log_data("Successfully connected to RETS.");
            return TRUE;
        } else {
            $error = $this->rets->Error();

            if($error['text'])
            {
                $error = $error['text'];
            } else {
                $error = "No error message returned from RETS. Check RETS debug file.";
            }

            $this->log_error("Failed to connect to RETS.\n".$error);
            die();
        }
    }


    private function get_properties_by_class()
    {
        $this->log_data("Getting Classes...");
        foreach ($this->config['property_classes'] as $class)
        {
            $this->log_data("Getting Class: ".$class);
            // Set
            $fields_order = array();
            $mod_timestamp_field = $this->get_timestamp_field($class);
            $previous_start_time = $this->config['previous_start_time'];
            $search_config = array("Format" => "COMPACT", "STATUS" => "STACT", "QueryType" => "DMQL2", "Limit" => 1);

            /*--------------------------------------------------------------------------------.
            |                                                                                 |
            | If you're having problems, they probably lie here in the $query and/or $search. |
            |                                                                                 |
            '--------------------------------------------------------------------------------*/

            // Query
            $query = "({$mod_timestamp_field}={$previous_start_time}+)";
//            $query = "(MLNumber=0+),({$mod_timestamp_field}={$previous_start_time}+)";

            // Run Search
            $search = $this->rets->SearchQuery("Property", $class, $query, array('Limit' => 1000, 'Offset' => 1, 'Format' => 'COMPACT-DECODED', 'Count' => 1));
//            $search = $this->rets->SearchQuery("Property", $class, $query, $search_config);


            // Get all active listings
            $query_all = "({$mod_timestamp_field}=1980-01-01T00:00:00+)";
            $search_all = $this->rets->SearchQuery("Property", $class, $query_all, array('Format'=>'COMPACT', 'Select'=>$this->config['KeyField']));
            $tmpArray = array();
            while($active_rid = $this->rets->FetchRow($search_all)) {
                array_push($tmpArray, $active_rid[$this->config['KeyField']]);
            }
            $this->active_ListingRids['property_'.strtolower($class)] = $tmpArray;


            $data = array();
            if ($this->rets->NumRows($search) > 0)
            {
                // Get columns
                $fields_order = $this->rets->SearchGetFields($search);

                $this->data['headers'] = $fields_order;

                // Process results
                while ($record = $this->rets->FetchRow($search))
                {
                    $this_record = array();

                    // Loop it
                    foreach ($fields_order as $fo)
                    {
                        $this_record[$fo] = $record[$fo];
                    }
                    $ListingRid = $record[$this->config['KeyField']];
                    $this_record['Photos'] = implode(',', $this->get_photos($ListingRid));
                    $data[] = $this_record;
                }
            }

            // Set data
            $this->data['classes'][$class] = $data;

            $this->log_data("Finished Getting Class: ".$class . "\nTotal found: " .$this->rets->TotalRecordsFound());

            // Free RETS Result
            $this->rets->FreeResult($search);
        }
    }

    private function get_timestamp_field($class)
    {
        $class = strtolower($class);

        switch($class)
        {
            case 'com':
                $field = "ModificationTimestamp";
                break;

            case 'res':
                $field = "ModificationTimestamp";
                break;
        }

        return $field;
    }


    private function get_photos($ListingRid)
    {
        $photos = $this->rets->GetObject("Property", "Photo", $ListingRid);
        $this_photos = array();
        foreach ($photos as $photo) {
            if(isset($photo['Content-ID']) && isset($photo['Object-ID']))
            {
                $listing = $photo['Content-ID'];
                $number = $photo['Object-ID'];

                if ($photo['Success'] == true) {
                    $this_photos[] = "image-{$listing}-{$number}.jpg";
                    file_put_contents($this->config['images_path']."image-{$listing}-{$number}.jpg", $photo['Data']);
                }
                else {
                    $this->log_data("Photo Error: ({$listing}-{$number}): {$photo['ReplyCode']} = {$photo['ReplyText']}\n");
                }
            }
        }
        return $this_photos;
    }


    private function disconnect()
    {
        $this->log_data("Disconnected from RETS.");
        $this->rets->Disconnect();
    }

    private function database_connect()
    {
        $this->log_data("Connecting to database...");

        $host     = $this->database['host'];
        $username = $this->database['username'];
        $password = $this->database['password'];
        $database = $this->database['database'];

        // Create connection
        $this->mysqli = new mysqli($host, $username, $password, $database);

        // Throw error if connection fails
        if ($this->mysqli->connect_error) {
            $this->log_error("Database Connection Error". $this->mysqli->connect_error);
            die('Connect Error (' . $this->mysqli->connect_errno . ') '
                . $this->mysqli->connect_error);
        }
    }

    private function file_delete_photos()
    {
        $this->log_data("Deleting expired photos");
        $tables = array('rets_property_resi');
        $count = 0;

        // Loop through each table and update
        foreach($this->config['property_classes'] as $class)
        {
            // Get Tables
            $table = "rets_property_".strtolower($class);
            $activeListings = $this->active_ListingRids['property_'.strtolower($class)];

            $sql = "SELECT Photos FROM {$table} WHERE {$this->config['KeyField']} NOT IN (".implode(',', $activeListings).");";
            $result = $this->mysqli->query($sql);
            while ($photos = $result->fetch_assoc()) {
                $files = explode(',', $photos['Photos']);

                foreach($files as $file)
                {
                    if(file_exists($this->config['images_path'].$file))
                    {
                        $count++;
                        unlink($this->config['images_path'].$file);
                    }
                }
            }
        }

        $this->log_data("Deleted {$count} photos.");
    }


    private function database_delete_records()
    {
        $this->log_data("Updating database...");

        // Loop through each table and update
        foreach($this->config['property_classes'] as $class)
        {
            // Get Tables
            $table = "rets_property_".strtolower($class);
            $activeListings = $this->active_ListingRids['property_'.strtolower($class)];
            $sql = "DELETE FROM {$table} WHERE {$this->config['KeyField']} NOT IN (".implode(',', $activeListings).");";

            $this->mysqli->query($sql);

            if($this->mysqli->affected_rows > 0)
            {
                $this->log_data("Deleted {$this->mysqli->affected_rows} Listings.");
//                return TRUE;
            } else if($this->mysqli->affected_rows == 0) {
                $this->log_data("Deleted {$this->mysqli->affected_rows} Listings.");
            } else {
                $this->log_data("Deleting database records failed \n\n" . mysqli_error($this->mysqli));
//                return FALSE;
            }
        }
    }


    private function database_insert_records()
    {
        $this->log_data("Inserting records...");

        foreach($this->config['property_classes'] as $class)
        {
            // Get Tables
            $table = "rets_property_".strtolower($class);

            // Get data
            $data_row = $this->data['classes'][$class];

            // Defaults
            $total_rows = 0;
            $total_affected_rows = 0;

            // Loop through data
            foreach($data_row as $drow)
            {
                // Clean data
                // replace empty with NULL
                // and wrap data in quotes

                $columns = array();
                $values = array();

                foreach($drow as $key => $val)
                {
                    if($val === '')
                    {
                        $val = '""';
                    } else {
                        $val = mysqli_real_escape_string($this->mysqli ,$val);
                        $val = "'$val'";
                    }
                    $columns[] = $key;
                    $values[] = $val;
                }

                // Implode data rows with commas
                $values = implode(', ', $values);
                $columns = implode(', ', $columns);

                // Build SQL
                $sql = "REPLACE INTO {$table} ({$columns}) VALUES ({$values})";

                // Do query
                $this->mysqli->query($sql);

                if($this->mysqli->affected_rows > 0)
                {
                    $total_affected_rows++;
                } else {
                    $this->log_error("Failed to insert the following record: ".$sql . "\n\n" . mysqli_error($this->mysqli));
                }
                $total_rows++;
            }
            $this->log_data("Done inserting data. ".$class."\nTotal Records: ".$total_rows." .\nTotal Inserted: ".$total_affected_rows);
        }
    }


    private function database_disconnect()
    {
        $this->log_data("Database disconnected...");
        // Close connection
        $this->mysqli->close();
    }

    private function create_table_for_property_class($class)
    {
        // gets resource information.  need this for the KeyField
        $rets_resource_info = $this->rets->GetMetadataInfo();

        $resource = "Property";

        // pull field format information for this class
        $rets_metadata = $this->rets->GetMetadata($resource, $class);

        $table_name = "rets_".strtolower($resource)."_".strtolower($class);
        // i.e. rets_property_resi

        $sql = $this->create_table_sql_from_metadata($table_name, $rets_metadata, $rets_resource_info[$resource]['KeyField']);
        $this->mysqli->query($sql);
    }

    private function create_table_sql_from_metadata($table_name, $rets_metadata, $key_field, $field_prefix = "")
    {
        $sql_query = "CREATE TABLE {$table_name} (\n";

        foreach ($rets_metadata as $field) {

            $field['SystemName'] = "`{$field_prefix}{$field['SystemName']}`";

            $cleaned_comment = addslashes($field['LongName']);

            $sql_make = "{$field['SystemName']} ";

            if ($field['Interpretation'] == "LookupMulti") {
                $sql_make .= "TEXT";
            }
            elseif ($field['Interpretation'] == "Lookup") {
                $sql_make .= "VARCHAR(50)";
            }
            elseif ($field['DataType'] == "Int" || $field['DataType'] == "Small" || $field['DataType'] == "Tiny") {
                $sql_make .= "INT({$field['MaximumLength']})";
            }
            elseif ($field['DataType'] == "Long") {
                $sql_make .= "BIGINT({$field['MaximumLength']})";
            }
            elseif ($field['DataType'] == "DateTime") {
                $sql_make .= "DATETIME default '0000-00-00 00:00:00' not null";
            }
            elseif ($field['DataType'] == "Character" && $field['MaximumLength'] <= 255) {
                $sql_make .= "VARCHAR({$field['MaximumLength']})";
            }
            elseif ($field['DataType'] == "Character" && $field['MaximumLength'] > 255) {
                $sql_make .= "TEXT";
            }
            elseif ($field['DataType'] == "Decimal") {
                $pre_point = ($field['MaximumLength'] - $field['Precision']);
                $post_point = !empty($field['Precision']) ? $field['Precision'] : 0;
                $sql_make .= "DECIMAL({$field['MaximumLength']},{$post_point})";
            }
            elseif ($field['DataType'] == "Boolean") {
                $sql_make .= "CHAR(1)";
            }
            elseif ($field['DataType'] == "Date") {
                $sql_make .= "DATE default '0000-00-00' not null";
            }
            elseif ($field['DataType'] == "Time") {
                $sql_make .= "TIME default '00:00:00' not null";
            }
            else {
                $sql_make .= "VARCHAR(255)";
            }

            $sql_make .= " COMMENT '{$cleaned_comment}'";
            $sql_make .= ",\n";

            $sql_query .= $sql_make;
        }

        $sql_query .= "`Photos` TEXT COMMENT 'Photos Array', ";
        $sql_query .= "PRIMARY KEY(`{$field_prefix}{$key_field}`) )";

        return $sql_query;
    }

    private function get_previous_start_time()
    {
        $filename = "previous_start_time.txt";

        // See if file exists
        if(file_exists($this->config['start_times_path'].$filename))
        {
            $time=time();
            $this->updates_log = fopen($this->config['start_times_path'].$filename, "r+");
            $this->previous_start_time = fgets($this->updates_log);
            $this->current_start_time = date("Y-m-d", $time) . 'T' . date("H:i:s", $time);

        } else {

            // Create file
            $this->updates_log = fopen($this->config['start_times_path'].$filename, "w+");
            fwrite($this->updates_log, "1980-01-01T00:00:00\n");
            $this->get_previous_start_time();
        }
        // fgets reads up to & includes the first newline, strip it
        return str_replace("\n", '', $this->previous_start_time);
    }

    private function set_previous_start_time()
    {
        $file = $this->config['start_times_path'] . "previous_start_time.txt";
        $file_data = $this->current_start_time."\n";
        $file_data .= file_get_contents($file);
        file_put_contents($file, $file_data);
    }


    private function logging_start()
    {
        $filename = "Log_".date("m-d-y_h-i-s", time()).".txt";

        // See if file exists
        if(file_exists($this->config['logs_path'].$filename))
        {
            $this->log = fopen($this->config['logs_path'].$filename, "a");

        } else {

            // Create file
            $this->log = fopen($this->config['logs_path'].$filename, "w+");
        }
    }

    private function log_data($data)
    {
        $write_data = "\nInfo Message: [".date("m/d/y - h:i:s", time())."]\n------------------------------------------------\n";
        $write_data .= $data."\n";
        $write_data .= "\n------------------------------------------------\n";
        fwrite($this->log, $write_data);

        if($this->config['to_screen'])
        {
            echo str_replace(array("\n"), array('<br />'), $write_data);
        }
    }

    private function log_error($error)
    {
        $write_data = "\nError Message: [".date("m/d/y - h:i:s", time())."]\n------------------------------------------------\n";
        $write_data .= $error."\n";
        $write_data .= "\n------------------------------------------------\n";
        fwrite($this->log, $write_data);

        if($this->config['to_screen'])
        {
            echo str_replace(array("\n"), array('<br />'), $write_data);
        }
    }

    private function logging_end()
    {
        $this->scriptend = date("m-d-y_h-i-s", time());
        $this->log_data("Closing log file.\n
                         Start Time: {$this->scriptstart}\n
                         End Time: {$this->scriptend}");
        fclose($this->log);
    }
}

// Load the class
$retshelper = new RETSHELPER;