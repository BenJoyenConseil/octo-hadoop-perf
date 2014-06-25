REGISTER wikipedia.jar;
DEFINE CustomInput wikipedia.pig.FileNameTextLoadFunc('20140605');
DEFINE SIM wikipedia.pig.SimilarityFunc();

records = LOAD $in USING CustomInput AS (filename: chararray, lang: chararray, page: chararray, visit: long);

filterbylang = FILTER records BY lang == 'fr' OR lang == 'en' OR lang == 'de';

restrictions = LOAD $restrictionFile USING TextLoader() AS (page: chararray);

joinbypage = JOIN filterbylang BY page LEFT OUTER, restrictions BY page USING 'replicated';

deleteRestrictedWords = FILTER joinbypage BY SIM(filterbylang::page, restrictions::page) == false;

groupbypage = COGROUP deleteRestrictedWords BY (filterbylang::page, filterbylang::lang);

sumrecords = FOREACH groupbypage GENERATE group.lang, group.page, SUM(deleteRestrictedWords.filterbylang::visit) AS visit_sum;

groupbylang  = GROUP sumrecords BY lang;

top20 = FOREACH groupbylang  {
	sorted = ORDER sumrecords BY visit_sum DESC;
	top1 = LIMIT sorted 20;
	GENERATE group, flatten(top1);
};


store top20 into $out;
