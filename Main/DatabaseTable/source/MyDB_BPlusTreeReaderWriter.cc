
#ifndef BPLUS_C
#define BPLUS_C

#include <algorithm>  // for sort
#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include "MyDB_PageListIteratorAlt.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe,
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	// the list of pages that we need to iterate
	vector <MyDB_PageReaderWriter> list;

	MyDB_INRecordPtr inLow = getINRecord ();
	MyDB_INRecordPtr inHigh = getINRecord ();
	inLow->setKey (low);
	inHigh->setKey (high);

	// fill up the list with pages
	discoverPages (rootLocation, list, low, high);

	MyDB_RecordPtr tempRec = getEmptyRecord ();

	function <bool ()> lowComparator = buildComparator (tempRec, inLow);
	function <bool ()> highComparator = buildComparator (inHigh, tempRec);

	MyDB_RecordPtr lhs = getEmptyRecord ();
	MyDB_RecordPtr rhs = getEmptyRecord ();

	function <bool ()> comparator = buildComparator (lhs, rhs);

	//    cout << "head: " << low->toInt() << "\n";
//	    cout << "tail: " << high->toInt() << "\n";

	// build the iterator
	MyDB_RecordIteratorAltPtr ans = make_shared <MyDB_PageListIteratorSelfSortingAlt> (list, lhs, rhs, comparator, tempRec, lowComparator, highComparator, true);
	return ans;

}




MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	vector <MyDB_PageReaderWriter> list;

	MyDB_INRecordPtr inLow = getINRecord ();
	MyDB_INRecordPtr inHigh = getINRecord ();
	inLow->setKey (low);
	inHigh->setKey (high);

	// fill up the list with pages
	discoverPages (rootLocation, list, low, high);

	MyDB_RecordPtr tempRec = getEmptyRecord ();

	function <bool ()> lowComparator = buildComparator (tempRec, inLow);
	function <bool ()> highComparator = buildComparator (inHigh, tempRec);

	MyDB_RecordPtr lhs = getEmptyRecord ();
	MyDB_RecordPtr rhs = getEmptyRecord ();

	function <bool ()> comparator = buildComparator (lhs, rhs);

	//    cout << "head: " << low->toInt() << "\n";
//	    cout << "tail: " << high->toInt() << "\n";

	// build the iterator
	MyDB_RecordIteratorAltPtr ans = make_shared <MyDB_PageListIteratorSelfSortingAlt> (list, lhs, rhs, comparator, tempRec, lowComparator, highComparator, false);
	return ans;

}

//MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
//
//	// this is the list of pages that we need to iterate over
//	vector <MyDB_PageReaderWriter> list;
//
//	discoverPages(rootLocation, list, low, high);
//
//	// for various comparisons
//	MyDB_RecordPtr lhs = getEmptyRecord ();
//	MyDB_RecordPtr rhs = getEmptyRecord ();
//	MyDB_RecordPtr myRec = getEmptyRecord ();
//	MyDB_INRecordPtr llow = getINRecord ();
//	llow->setKey (low);
//	MyDB_INRecordPtr hhigh = getINRecord ();
//	hhigh->setKey (high);
//
//	// build the comparison functions
//	function <bool ()> comparator = buildComparator (lhs, rhs);
//	function <bool ()> lowComparator = buildComparator (myRec, llow);
//	function <bool ()> highComparator = buildComparator (hhigh, myRec);
//
//	// and build the iterator
//	return make_shared <MyDB_PageListIteratorSelfSortingAlt> (list, lhs, rhs, comparator, myRec, lowComparator, highComparator, false);
//}



bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {

	// target page
	MyDB_PageReaderWriter target = (*this)[whichPage];

	// regular page
	if(target.getType() != RegularPage) {
		// iterate subtrees


		int i = -1;

		vector<int> tempList;

		MyDB_INRecordPtr inLow = getINRecord();
		MyDB_INRecordPtr inHigh = getINRecord();
		inLow -> setKey(low);
		inHigh -> setKey(high);

		MyDB_INRecordPtr tempRec = getINRecord();


		function<bool ()> highComparator = buildComparator(inHigh, tempRec);
		function<bool ()> lowComparator = buildComparator(tempRec, inLow);


		MyDB_RecordIteratorAltPtr rec = target.getIteratorAlt();

		while(true) {

			if(!rec -> advance()){
				break;
			}

			rec -> getCurrent(tempRec);
			if(i == -1 && !lowComparator()){
				i = tempRec -> getPtr();
			}
			if(i != -1){
				tempList.push_back(tempRec->getPtr());
				if(highComparator()) {
					break;
				}
			}
		}

		// discover recursively
		for(vector<int>::iterator it= tempList.begin(); it != tempList.end(); it++) {
			discoverPages(*it, list, low, high);
		}

		return false;

	}
	// internal node, search the subtree
	else {
		list.push_back(target);
		return true;
	}
}


void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr newRec) {

	if (getNumPages () > 1) {

		MyDB_RecordPtr ans = append (rootLocation, newRec);

		if (ans == nullptr) {}
		else {

			int newLoc = getTable () -> lastPage () + 1;

			getTable ()->setLastPage (newLoc);
			MyDB_PageReaderWriter base = (*this)[newLoc];

			base.clear ();
			base.setType (DirectoryPage);

			base.append (ans);
			MyDB_INRecordPtr newRec = getINRecord ();

			newRec -> setPtr (rootLocation);
			base.append (newRec);

			rootLocation = newLoc;
		}

	}
	else {
		MyDB_PageReaderWriter root = (*this)[0];
		rootLocation = 0;

		MyDB_INRecordPtr rec = getINRecord ();
		rec -> setPtr (1);
		getTable () -> setLastPage (1);

		MyDB_PageReaderWriter child = (*this)[1];
		child.clear ();

		child.append (newRec);
		child.setType (RegularPage);

		root.clear ();
		root.append (rec);
		root.setType (DirectoryPage);
	}
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter targetPage, MyDB_RecordPtr newRec) {

	MyDB_PageType type;
	MyDB_RecordPtr left, right;
	vector <void *> loc;
	function <bool ()> comparator;

	int newLocation = getTable() -> lastPage () + 1;
	getTable() -> setLastPage(newLocation);
	MyDB_PageReaderWriter newPage = (*this)[newLocation];

	if (targetPage.getType () == DirectoryPage) {
		left = getINRecord ();
		right = getINRecord ();
		type = DirectoryPage;
		comparator = buildComparator (left, right);

	}
	else if (targetPage.getType () == RegularPage ) {
		left = getEmptyRecord ();
		right = getEmptyRecord ();
		type = RegularPage;
		comparator = buildComparator (left, right);

	}

	RecordComparator newCompare (comparator, left, right);

	void *tempPage = malloc (targetPage.getPageSize ());
	memcpy (tempPage, targetPage.getBytes (), targetPage.getPageSize ());

	int used = sizeof(size_t)*2;

	while (true) {
		if (used == *((size_t *) ((sizeof(size_t) +(char *) tempPage)))) {
			break;
		}
		void* newloc = used + (char*) tempPage;

		void* next = left -> fromBinary(newloc);

		used += ((char*) next) - ((char*) newloc);
		loc.push_back (newloc);
	}

	void *tempSpace = malloc (newRec -> getBinarySize ());
	newRec -> toBinary (tempSpace);
	loc.push_back (tempSpace);

	std::sort (loc.begin (), loc.end (), newCompare);

	newPage.clear ();
	targetPage.clear ();
	newPage.setType (type);
	targetPage.setType (type);

	MyDB_INRecordPtr res = getINRecord ();
	res->setPtr (newLocation);

	int count = -1;
	int size = loc.size();
	
	for(int i = 0; i < loc.size(); i++){

		count++;

		left -> fromBinary (loc[i]);

		if (count > size / 2)
			targetPage.append (left);


		if (count < size / 2)
			newPage.append (left);


		if (count == size / 2) {
			newPage.append (left);
			res -> setKey (getKey (left));
		}

	}

	free (tempSpace);
	free (tempPage);

	return res;

}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr newRec) {
	MyDB_PageReaderWriter targetPage = (*this)[whichPage];
	//	if is a internal node, find the subtree to insert to
	if(targetPage.getType() != RegularPage) {
		int nextPage = -1;
		MyDB_RecordIteratorAltPtr it = targetPage.getIteratorAlt();
		MyDB_INRecordPtr rec = getINRecord();
		function<bool()> compare = buildComparator(rec, newRec);
		// find where to insert
		while(true) {
			if(!it -> advance()) {
				break;
			}
			it -> getCurrent(rec);
			if(!compare()) {
				nextPage = rec -> getPtr();
				break;
			}
		}

		// insert
		MyDB_RecordPtr ans = append(nextPage, newRec);
		if(ans != nullptr) {
			//	insert the node to the page and sort;
			if(!targetPage.append(ans)) {

				MyDB_RecordPtr res = split(targetPage, ans);
				return res;

			}

			else {
				MyDB_INRecordPtr left = getINRecord();
				MyDB_INRecordPtr right = getINRecord();
				targetPage.sortInPlace(buildComparator (left, right), left, right);
				return nullptr;
			}
		}
		else {
			return nullptr;
		}
	}
	//	if is regularpage
	else {
		//	if the regularpage need to be split
		if(!targetPage.append(newRec)) {
			MyDB_RecordPtr res = split(targetPage, newRec);
			return res;

		}
		//	if this regularpage can insert the newRec successfully
		else {
			return nullptr;
		}

	}
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
	queue<MyDB_PageReaderWriterPtr> que;

	que.push(getWithID(getTable()->getRootLocation()));
	while(que.size()!=0){
		MyDB_PageReaderWriterPtr temp = que.front();
		auto pageIter = temp -> getIteratorAlt();
		que.pop();

		if(temp->getType() != DirectoryPage){
			while (pageIter -> advance()) {
				auto rec = getEmptyRecord();
				pageIter -> getCurrent(rec);
				cout << rec -> getAtt(whichAttIsOrdering)->toString() << " ";

			}
		}
		else {
			while(pageIter -> advance()){
				auto rec = getINRecord();
				pageIter -> getCurrent(rec);
				que.push(make_shared <MyDB_PageReaderWriter> (*this, rec -> getPtr()));
				cout << rec -> getKey() -> toString() << " ";
			}
		}
	}
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr)
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}

	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

MyDB_PageReaderWriterPtr MyDB_BPlusTreeReaderWriter :: getWithID(int index) {
	//cout << "go to ptr: " << index << "\n";
	//see if we are going off of the end of the file... if so, then clear those pages
	while (index > forMe->lastPage ()) {
		forMe->setLastPage (forMe->lastPage () + 1);
		lastPage = make_shared <MyDB_PageReaderWriter> (*this, forMe->lastPage ());
		lastPage->clear ();
	}
	// now get the page
	MyDB_PageReaderWriterPtr child = make_shared <MyDB_PageReaderWriter> (*this, index);
	return child;
}

#endif


