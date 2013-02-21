function(doc) { 
    if(doc.doc_type == "RDFEntity" && doc.o) { 
	 if( doc.o.length > 0 ) {
		for( var i=0; i<doc.o.length; ++i) {
//		 	       emit('OBJ'+doc.o[i], {_id:'OBJ'+doc.o[i], s:doc.s[i], p:doc.p[i], doc_type:doc.doc_type});
//		    	       emit(doc.o[i], {s:doc.s, p:doc.p[i]});
		
			// don't need to include subject as value as it will appear instead of 'id' 
			if( doc.o[i].substr(0,7) == "http://" || doc.o[i].substr(0,8) == "https://" ) {  
				emit(doc.o[i], {p:doc.p[i]});
			}
		}
	}
	}   
}
