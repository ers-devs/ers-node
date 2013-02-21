function(doc) { 
       if(doc.doc_type == "RDFEntity" && doc.o) { 
		var obj = doc.o;
		if( doc.p.length > 0 ) { 
			for( var i = 0; i<doc.p.length; ++i ) { 
				emit(doc.p[i], {o:doc.o[i]});
			}
		}
	}
}
