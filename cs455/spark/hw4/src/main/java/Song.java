/*
 * This class is used by Beans.encoder to transform the data.
 * Victor Weeks, Diego Batres, Josiah May
 * 
 */


import Util.MusicGenreTerms;
import Util.RowParser;
import java.io.Serializable;

/**
 * Format for encoding songs
 */
public class Song implements Serializable {

  private String artist_terms;
  private double duration;
  private double end_of_fade_in;
  private int key;
  private double loudness;
  private int mode;
  private double start_of_fade_out;
  private double tempo;
  private int time_signature;
  private int year;
  private double[] sections_start;
  private double[] segments_start;
  private double[] segments_timbre;
  private double[] tatums_start;
  private double[] bars_start;
  private double[] beats_start;
  private double[] segments_loudness_max;
  private double[] segments_pitches;
  private int segments_length;
  private int sections_length;
  private int bars_length;
  private int beats_length;


  /**
   * Gets the top term the artist has listed
   *
   * @return the term
   */
  public String getArtist_Terms() {
    return artist_terms;
  }

  /**
   * Sets the first term of the artist
   *
   * @param artist_terms All the terms of artist in a coma delinted string
   */
  public void setArtist_Terms(String artist_terms) {
    this.artist_terms = classifyGenre(artist_terms);
  }

  /**
   * Gets length of the song
   *
   * @return song length
   */
  public double getDuration() {
    return duration;
  }

  /**
   * Sets the length of the song
   *
   * @param duration song length
   */
  public void setDuration(double duration) {
    this.duration = duration;
  }

  /**
   * Gets the start of the song fade in
   *
   * @return the fade in time
   */
  public double getEnd_Of_Fade_In() {
    return end_of_fade_in;
  }

  /**
   * Sets the start of the song fade in
   *
   * @param end_of_fade_in the fade in time
   */
  public void setEnd_Of_Fade_In(double end_of_fade_in) {
    this.end_of_fade_in = end_of_fade_in;
  }

  /**
   * Get the key the song is made in
   *
   * @return the key
   */
  public int getKey() {
    return key;
  }

  /**
   * Sets the key the song is made in
   *
   * @param key the key
   */
  public void setKey(int key) {
    this.key = key;
  }

  /**
   * Gets the over all loudness of the song
   *
   * @return the loudness of the song
   */
  public double getLoudness() {
    return loudness;
  }

  /**
   * Sets the over all loudness of the song
   *
   * @param loudness the loudness of the song
   */
  public void setLoudness(double loudness) {
    this.loudness = loudness;
  }

  /**
   * Gets the mode (major/minor) the song is in
   *
   * @return the mode
   */
  public int getMode() {
    return mode;
  }

  /**
   * Gets the mode (major/minor) the song is in
   *
   * @param mode the mode
   */
  public void setMode(int mode) {
    this.mode = mode;
  }

  /**
   * Gets the time the song starts to fade out
   *
   * @return fade out time
   */
  public double getStart_Of_Fade_Out() {
    return this.start_of_fade_out;
  }

  /**
   * Sets the time the song starts to fade out
   *
   * @param start_of_fade_out fade out time
   */
  public void setStart_Of_Fade_Out(double start_of_fade_out) {
    this.start_of_fade_out = start_of_fade_out;
  }

  /**
   * Gets the BPM of the song
   *
   * @return the BPM
   */
  public double getTempo() {
    return tempo;
  }

  /**
   * Sets the BPM of the song
   *
   * @param tempo the BPM
   */
  public void setTempo(double tempo) {
    this.tempo = tempo;
  }

  /**
   * Gets the beats per bar of the song
   *
   * @return the beats per bar
   */
  public int getTime_Signature() {
    return time_signature;
  }

  /**
   * Sets the beats per bar of the song
   *
   * @param time_signature the beats per bar
   */
  public void setTime_Signature(int time_signature) {
    this.time_signature = time_signature;
  }

  /**
   * Gets the year the song was made
   *
   * @return the year
   */
  public int getYear() {
    return year;
  }

  /**
   * Gets the year the song was made
   *
   * @param year the year
   */
  public void setYear(int year) {
    this.year = year;
  }

  /**
   * Gets the mean difference of the time between segment starts. It is a single double but stored
   * in an array for encoder to work
   *
   * @return the mean difference
   */
  public double[] getSegments_start() {
    return segments_start;
  }

  /**
   * Sets the mean difference of the time between segment starts. It also sets the total number
   * segments
   */
  public void setSegments_start(double[] segments_start) {
    this.segments_start = getMeanOfArray(segments_start);
    this.segments_length = segments_start.length;
  }

  /**
   * Gets the mean difference of the bars timbre per note (12)
   *
   * @return the mean difference of the bars timbre
   */
  public double[] getSegments_timbre() {
    return segments_timbre;
  }

  /**
   * Sets the mean difference of the bars timbre per note (12)
   *
   * @param segments_timbre A 2d array of all the timbre notes
   */
  public void setSegments_timbre(double[] segments_timbre) {
    this.segments_timbre = findTimbreDifference(segments_timbre);
  }

  /**
   * Gets the mean difference of the time between Tatums starts. It is a single double but stored in
   * an array for encoder to work
   *
   * @return the mean difference
   */
  public double[] getTatums_start() {
    return tatums_start;
  }

  /**
   * Sets the mean difference of the time between Tatums starts
   */
  public void setTatums_start(double[] tatums_start) {
    this.tatums_start = getMeanOfArray(tatums_start);
  }

  /**
   * Gets the mean difference of the time between bar starts. It is a single double but stored in an
   * array for encoder to work
   *
   * @return the mean difference
   */
  public double[] getBars_start() {
    return bars_start;
  }

  /**
   * Sets the mean difference of the time between bar starts. It also sets the total number of bars
   */
  public void setBars_start(double[] bars_start) {
    this.bars_start = getMeanOfArray(bars_start);
    bars_length = bars_start.length;
  }

  /**
   * Gets the mean difference of the time between beats starts. It is a single double but stored in
   * an array for encoder to work
   *
   * @return the mean difference
   */
  public double[] getBeats_start() {
    return beats_start;
  }

  /**
   * Sets the mean difference of the time between bets starts It also sets the total number of
   * beats
   */
  public void setBeats_start(double[] beats_start) {
    this.beats_start = getMeanOfArray(beats_start);
    beats_length = beats_start.length;
  }

  /**
   * Gets the mean difference of the time between the max loudness in the segments. It is a single
   * double but stored in an array for encoder to work
   *
   * @return the mean difference
   */
  public double[] getSegments_loudness_max() {
    return segments_loudness_max;
  }

  /**
   * Gets the mean difference of the time between the max loudness in the segments
   *
   * @param segments_loudness_max the mean difference
   */
  public void setSegments_loudness_max(double[] segments_loudness_max) {
    this.segments_loudness_max = getMeanOfArray(segments_loudness_max);
  }

  /**
   * Gets the average pitch for all the segments (12)
   *
   * @return the mean difference of the bars timbre
   */
  public double[] getSegments_pitches() {
    return segments_pitches;
  }

  /**
   * Sets the average pitch for all the segments (12)
   *
   * @param segments_pitches A 2d array of all the segments pitches
   */
  public void setSegments_pitches(double[] segments_pitches) {
    this.segments_pitches = findAveragePitches(segments_pitches);
  }

  /**
   * Gets the mean difference of the time between the sections starts. It is a single double but
   * stored in an array for encoder to work
   *
   * @return the mean difference
   */
  public double[] getSections_start() {
    return sections_start;
  }

  /**
   * Sets the mean difference of the time between the sections starts. It also sets the total number
   * segments
   */
  public void setSections_start(double[] sections_start) {
    this.sections_start = getMeanOfArray(sections_start);
    sections_length = sections_start.length;
  }

  /**
   * Finds the mean difference for all the items in an array. It returns the answer in a double[1] array because encoder need same data types
   * @param data the array of data
   * @return the mean difference
   */
  private double[] getMeanOfArray(double[] data) {
    // Vars to save cpu and memory allocations
    double total = 0.0;
    int size = data.length - 1;
    double diff;
    // Loop through the array
    for (int i = 0; i < size; i++) {
      diff = data[i + 1] - data[i]; // find diff
      total += diff; // sum up the diffs
    }
    // divide the total by the size to get the mean
    return new double[]{ total / size};
  }

  /**
   * Gather all the features of the song into a double[] to make a vector for the mlb models
   * @return all the features
   */
  public double[] getFeatures() {
    // All the individual items
    double[] rt = {duration, end_of_fade_in, (double) key, loudness, (double) mode,
        start_of_fade_out, tempo, (double) time_signature,
        (double) year, sections_start[0], segments_start[0], tatums_start[0],
        bars_start[0], beats_start[0], segments_loudness_max[0], (double) segments_length,
        (double) sections_length, (double) bars_length, (double) beats_length};

    // combine the two feature that are already arrays
    double[] rt2 = RowParser.combineDoubles(segments_timbre, segments_pitches);

    // Return the combined arrays of features
    return RowParser.combineDoubles(rt, rt2);
  }

  /**
   * Finds the average of all the segments pitches
   * @param data a 2d array of size[?][12]
   * @return the mean average for all twelve pitches double[12]
   */
  private double[] findAveragePitches(double[] data) {
    // Vars
    double[] rt = new double[12];
    int size = data.length;
    // Loop through all array and add each pitch to the total for the pitch
    for (int i = 0; i < size; i++) {
      rt[i % 12] += data[i];
    }
    // divide the results by the total number of segment to get the average
    return divideArray(rt, data.length / 12);
  }

  /**
   * Finds the mean average of all the segments timbre
   * @param data a 2d array of size[?][12]
   * @return the mean average for all twelve pitches double[12]
   */
  private double[] findTimbreDifference(double[] data) {
    // Vars
    double[] rt = new double[12];
    int size = data.length - 12; // stop 12 early to stay in array limit
    double diff;
    // Loop through all array and check 12 items ahead
    for (int i = 0; i < size; i++) {
      diff = data[i] - data[i + 12]; // find diff
      rt[i % 12] += diff; // mod to the correct storage location
    }
    // divide the results by the total number of segment
    return divideArray(rt, data.length / 12);
  }

  /**
   * Divides all of an array by a number
   * @param data the array to divide
   * @param div the number to divide by
   * @return the array
   */
  private double[] divideArray(double[] data, int div) {
    // Loop through the array and divide each element
    for (int i = 0; i < data.length; i++) {
      data[i] = data[i] / div;
    }
    return data;
  }

  /**
   * Returns the general genre of a term or null if it is not a common terms. Null is used for the dataset to filter out data not useful for the models
   * @param term the song genre to look for
   * @return the genre or null
   */
  private String classifyGenre(String term) {
    term = term.toLowerCase(); // should not be need

    // Check for the common terms
    if (term.contains("blues")) {
      return "Blues";
    } else if (term.contains("christian")) {
      return "Christian";
    } else if (term.contains("gospel")) {
      return "Christian";
    } else if (term.contains("country")) {
      return "Country";
    } else if (term.contains("house")) {
      return "Electronic";
    } else if (term.contains("trance")) {
      return "Electronic";
    } else if (term.contains("electro")) {
      return "Electronic";
    } else if (term.contains("punk")) {
      return "Punk";
    } else if (term.contains("metal")) {
      return "Metal";
    } else if (term.contains("rock")) {
      return "Rock";
    } else if (term.contains("hip hop")) {
      return "Hip-hop/rap";
    } else if (term.contains("rap")) {
      return "Hip-Hop/Rap";
    } else if (term.contains("soul")) {
      return "R&B/Soul";
    } else if (term.contains("jazz")) {
      return "Jazz";
    } else if (term.contains("pop")) {
      return "Pop";
    }

    // If the code reaches here check the map of other terms
    return termInList(term);
  }

  /**
   * Checks if the term is in the map of unique terms if not in the map null is returned
   * @param term the term to search for
   * @return the general term for the genre or null
   */
  private String termInList(String term) {

    return MusicGenreTerms.genre.get(term);
  }


}
