var _ = require("underscore"),
    converter = require("./../converter/wikiToHTMLConverter");

_.str = require("underscore.string");
_.mixin(_.str.exports());

function parseJson(wiki, lang, callback) {
	var metaDataCounter = 0;
	//Take the first result
	var latestArticle = wiki;
	//      console.log(latestArticle);
	var lines = latestArticle.split("\n");
	var linesToParse = [];
	var parsedText = "";
	var sectionLines = [];

	lines.forEach(function(line) {
		//Ignoring metadata information
		var matchArray = line.match(/\{\{/g);
		var openingDoubleCurlyBracketsCount = matchArray ? matchArray.length : 0;

		matchArray = line.match(/\}\}/g);
		var closingDoubleCurlyBracketsCount = matchArray ? matchArray.length : 0;

		if (openingDoubleCurlyBracketsCount > closingDoubleCurlyBracketsCount) {
			++metaDataCounter;
		} else if (closingDoubleCurlyBracketsCount > openingDoubleCurlyBracketsCount) {
			--metaDataCounter;
		}

		if (metaDataCounter == 0 && !(_(line).startsWith("}}") ||
			_(line).startsWith("|") ||
				_(line).startsWith("{") ||
				_(line).startsWith("<") ||
				_(line).startsWith("[[File") ||
				_(line).startsWith("\n") ||
				line.length == 0)) {
					sectionLines.push(line);
				}
	});

	//Add the section lines to the lines to parse array if it was not already done
	if (linesToParse.length == 0) {
		linesToParse.push(sectionLines);
	}

	linesToParse.forEach(function(sectionLines) {
		var list = false;
		sectionLines.forEach(function(line) {

			var convertedLine = converter.convertLineToHTML(line, lang);

			if (_(convertedLine).startsWith("*")) {
				if (!list) {
					list = true;
					parsedText += "<ul>\n";
				}
				parsedText += "<li>" + convertedLine.substring(1) + "</li>\n";
			} else if (_(convertedLine).startsWith("<h")) {
				if (list) {
					parsedText += "</ul>\n";
					list = false;
				}
				parsedText += convertedLine + "\n";
			} else {
				if (list) {
					parsedText += "</ul>\n";
					list = false;
				}
				parsedText += "<p>" + convertedLine + "</p>\n";
			}

		});
	});

	process.nextTick(function() {
		callback(null, parsedText);
	});

}


module.exports.parse = function(wiki, format, lang) {
  return new Promise((resolve, reject) => {
    parseJson(wiki, lang, (err, res) => {
      if (err)
        reject(err);
      resolve(res);
    });
  });
};
