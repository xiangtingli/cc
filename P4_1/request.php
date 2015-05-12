<?php
if ( !isset($_REQUEST['term']) )
{       echo 'error';
        exit;
}

include('./httpful.phar');

// EDIT these to match your schema
$tableName="tbl";
$columnFamily="word";

// trim leading and trailing spaces
$searchTerm = strtolower(trim( $_REQUEST['term'] ));

// Connect to HBASE server
//  note: we are running in non "security" mode, so no user auth needed
$url = "ec2-52-5-237-146.compute-1.amazonaws.com:8080/".$tableName."/".urlencode($searchTerm)."?v=1";

//Send Request 
$response = \Httpful\Request::get($url)->addHeader('Accept','application/json')->send();

//DEBUG
//echo $response;

// iterate through response, adding each to array
$data = array();
$json = json_decode($response,true);
$row=$json["Row"];
$Cell=$row[0]["Cell"];
$first=array();
$second=array();

//Need to check if we have recieved an array of results or just a single one
if(is_array($Cell[0])){
	foreach($Cell as $item) {

	    //DEBUG
	    //echo $item;

	    $word = str_replace("$columnFamily:","",base64_decode($item["column"]));
	    $first[] = $word;

	    //DEBUG - Adding word probablility
	    #echo $word;
	    $probability = base64_decode($item['$']);
            $second[] = $probability;

	}
}
else {
	    $word = str_replace("$columnFamily:","",base64_decode($Cell["column"]));
	    $first[] = $word;

	    //DEBUG - Adding word probablility
	    #echo $word;
	    $probability = base64_decode($Cell['$']);
            $second[] = $probability;
}

//Sort by probability first and then alphabetically
array_multisort($second, SORT_DESC, $first);
foreach(array_keys($first) as $key){
	    $data[] = array(
			    'label' => $first[$key],
			    'probability' => $second[$key]
	    );
}

// return JSON encoded array to the JQuery autocomplete function
echo json_encode($data);
flush();
?>

