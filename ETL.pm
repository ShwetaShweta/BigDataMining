#! /usr/bin/perl -w
use strict;
use JSON;
package ETL;
#Sub routine definition
sub extractRestaurants(){
	my($file,$file1) =@_;
	open(FHRead,"<$file") or die "\nCannot open file $file";
	print "\nExtracting business type 'Restaurants' from file $file";
	open(FHWrite,">$file1") or die "\nCannot open file $file1";
	while(my $row = <FHRead>){
		if($row =~/."Restaurants"/i){
			print FHWrite $row;
		}
	} 
	print "\nExtraction Complete,data written in file $file1 ";
	close(FHRead);
	close(FHWrite);
}


sub extractBusinessObjects(){
    my($readFile,$writeFile)=@_;	
 #Creating new Json object
    my $json = JSON->new->allow_nonref;
 #Opening files for read and write purposes
    open(FH,"<$readFile") or die("Can't open file $readFile for reading $!\n");
    open(FW,">$writeFile") or die("Can't open file $writeFile to write business_objects:$! ");
    while(my $json_text=<FH>){
    	#decoding json file line by line
        my $perl_scalar = $json->decode($json_text);
        #Dereferencing the output hash 
        my %hash = %$perl_scalar; 
        #Extracting required fields and storing in another hash
        	my $hash_ref={ business_id =>$hash{business_id},
          	             name =>$hash{name},
          	             categories=>$hash{categories},
          	             review_count=>$hash{review_count},
          	             stars=>$hash{stars}};
          	             #Encoding new hash to json
            my  $json_t=JSON::encode_json($hash_ref);
            #Writing data to file
            print FW $json_t."\n";
    }
close(FH);
close(FW);
printf "$writeFile file write Successful\n";
}
sub extractReviewObjects(){
    my($readFile,$writeFile,$stopWordsFile)=@_;	
 #Creating new Json object
    my $json = JSON->new->allow_nonref;
 #Opening files for read and write purposes
    open(FH,"<$readFile") or die("Can't open file $readFile for reading $!\n");
 #Reading Stop Words
    open(FS,"<$stopWordsFile") or die("Can't open file $stopWordsFile:$!");	
    my @StopWords;
    while(my $line=<FS>){
		chop($line);
		push(@StopWords,$line);
	}
	close(FS);
    open(FW,">$writeFile") or die("Can't open file $writeFile to write business_objects:$! ");
    while(my $json_text=<FH>)
    {
    	#decoding json file line by line
        my $perl_scalar = $json->decode($json_text);
        #Dereferencing the output hash 
        my %hash = %$perl_scalar;          
        #Extracting reviews to perform natural language processing
        my $review = $hash{text};
        my $processedReview= nLP($review,@StopWords);
        #Extracting required fields and storing in another hash
        	my $hash_ref={ user_id =>$hash{user_id},
          	             stars =>$hash{stars},
          	             text=>$processedReview,
          	             date=>$hash{date},
          	             };
          	             #Encoding new hash to json
        #    print %hash ."\n";
         
            my  $json_t=JSON::encode_json($hash_ref);
            #Writing data to file
            print FW $json_t."\n";
    }
close(FH);
close(FW);
printf "$writeFile file Write Successful\n";
}
sub extractUserObjects(){
	 my($readFile,$writeFile)=@_;	
 #Creating new Json object
    my $json = JSON->new->allow_nonref;
 #Opening files for read and write purposes
    open(FH,"<$readFile") or die("Can't open file $readFile for reading $!\n");
    open(FW,">$writeFile") or die("Can't open file $writeFile to write business_objects:$! ");
    while(my $json_text=<FH>){
    	#decoding json file line by line
        my $perl_scalar = $json->decode($json_text);
        #Dereferencing the output hash 
        my %hash = %$perl_scalar; 
        #Extracting required fields and storing in another hash
        	my $hash_ref={ user_id =>$hash{user_id},
          	             name =>$hash{name},
          	             review_count=>$hash{review_count},
          	             average_stars=>$hash{average_stars}};
          	             #Encoding new hash to json
            my  $json_t=JSON::encode_json($hash_ref);
            #Writing data to file
            print FW $json_t."\n";
    }
close(FH);
close(FW);
printf "$writeFile file write Successful\n";
}


sub binarySearch{
	my($array,$str)=@_;
	my @array1 = @$array;
	my $low=0;
	my $high=$#array1 - 1;
	while($low<=$high){
	    my $mid=($low+$high)/2;
		my $mid_element = $array1[$mid];
		if($str lt $mid_element ){
			$high = $mid-1;
		}
		elsif ($str gt $mid_element ){
			$low = $mid+1;
		}
		else{
			return $mid;
		}
	}
	return -1;	
}
sub nLP(){
	my($review,@StopWords)=@_;
	my @Array = split(' ',$review);
	my @newArray;
	my $str='';
	foreach my $element(@Array){
		$element =~s/[^a-zA-Z']//g;
		my $ret = binarySearch(\@StopWords,$element);
		if($ret == -1){
	      $str=$str.",".$element;	
		}
	}
	return $str;
}


1;
