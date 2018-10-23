package edu.mcw.rgd.RNASeqPipeline;



import dragon.nlp.tool.Lemmatiser;
import dragon.nlp.tool.lemmatiser.EngLemmatiser;

/**
 *
 * @author majidrastegar-mojarad
 */
public class LemmatizerDragon {

	static private transient Lemmatiser lemmatiser = null;
	static private LemmatizerDragon singleton = null;

	static public LemmatizerDragon getInstance() {
		if (singleton != null) {
			return singleton;
		}
		singleton = new LemmatizerDragon();
		return singleton;
	}

	public LemmatizerDragon() {
		lemmatiser = new EngLemmatiser("data/lemmatiser", false, true);
	}

	public String LemmaSent(String text) {

		String sentLem = "";
		try {
			String[] words = text.split(" ");
			sentLem = LemmaWord(words[0]);
			for (int i = 1; i < words.length; i++) {
				sentLem = sentLem + " " + LemmaWord(words[i]);
			}
		} catch (Throwable t) {
			System.out.println("Error in LemmaSent in LemmatizerDragon");
			System.out.println("Error Message = " + t.getMessage());

		}
		return sentLem;
	}

	public String LemmaWord(String text) {
		String wordLemma = "";
		
		if(text!=null){
			try {
				wordLemma = lemmatiser.lemmatize(text);
			} catch (Throwable t) {
				System.out.println("Error in LemmaWord in LemmatizerDragon");
				System.out.println("Error Message = " + t.getMessage());

			}
		}
		return wordLemma;
	}
}
