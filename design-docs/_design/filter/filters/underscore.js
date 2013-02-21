function(doc, req) {
	if (doc.g && doc.g[0] != "_") {
		return true;
	} 
	else {
		return false;
	}
}
