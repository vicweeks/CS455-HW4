/*
 * This class is used by Beans.encoder to transform the data.
 * Victor Weeks, Diego Batres, Josiah May
 * 
 */


import Util.RowParser;
import java.io.Serializable;

public class Song implements Serializable {
    public String artist_terms;
    public double duration;
    public double end_of_fade_in;
    public int key;
    public double loudness;
    public int mode;
    public double start_of_fade_out;
    public double tempo;
    public int time_signature;
    public int year;
    public double sections_start;
    public double segments_start;
    public double[] segments_timbre;
    public double tatums_start;
    public double bars_start;
    public double beats_start;
    public double segments_loudness_max;
    public double[] segments_pitches;
    private int segments_length;
    private int sections_length;
    private int bars_length;
    private int beats_length;

    
    public String getArtist_Terms() { return artist_terms; }
    public void setArtist_Terms(String artist_terms) { this.artist_terms = artist_terms; }
    
    public double getDuration() { return duration; }
    public void setDuration(double duration) { this.duration = duration; }
    
    public double getEnd_Of_Fade_In() { return end_of_fade_in; }
    public void setEnd_Of_Fade_In(double end_of_fade_in) { this.end_of_fade_in = end_of_fade_in; }
    
    public int getKey() { return key; }
    public void setKey(int key) { this.key = key; }
	
    public double getLoudness() { return loudness; }
    public void setLoudness(double loudness) { this.loudness = loudness; }
    
    public int getMode() { return mode; }
    public void setMode(int mode) { this.mode = mode; }
	
    public double getStart_Of_Fade_Out() { return this.start_of_fade_out; }
    public void setStart_Of_Fade_Out(double start_of_fade_out) { this.start_of_fade_out = start_of_fade_out; }
    
    public double getTempo() { return tempo; }
    public void setTempo(double tempo) { this.tempo = tempo; }
	
    public int getTime_Signature() { return time_signature; }
    public void setTime_Signature(int time_signature) { this.time_signature = time_signature; }
    
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    public double getSegments_start() { return segments_start; }

    public void setSegments_start(double[] segments_start) {
        this.segments_start = getMeanOfArray(segments_start);
        this.segments_length = segments_start.length;
    }

    public double[] getSegments_timbre() {
        return segments_timbre;
    }

    public void setSegments_timbre(double[] segments_timbre) {
        this.segments_timbre = findTimbreDifference(segments_timbre);
    }

    public double getTatums_start() {
        return tatums_start;
    }

    public void setTatums_start(double[] tatums_start) {
        this.tatums_start = getMeanOfArray(tatums_start);
    }

    public double getBars_start() {
        return bars_start;
    }

    public void setBars_start(double[] bars_start) {
        this.bars_start = getMeanOfArray(bars_start);
        bars_length = bars_start.length;
    }

    public double getBeats_start() {
        return beats_start;
    }

    public void setBeats_start(double[] beats_start) {
        this.beats_start = getMeanOfArray(beats_start);
        beats_length = beats_start.length;
    }

    public double getSegments_loudness_max() {
        return segments_loudness_max;
    }

    public void setSegments_loudness_max(double[] segments_loudness_max) {
        this.segments_loudness_max = getMeanOfArray(segments_loudness_max);
    }

    public double[] getSegments_pitches() {
        return segments_pitches;
    }

    public void setSegments_pitches(double[] segments_pitches) {
        this.segments_pitches = findAveragePitches(segments_pitches);
    }

    private double getMeanOfArray(double[] data){
        double total = 0.0;
        int size = data.length -1;
        double diff = 0.0;
        for (int i = 0; i < size; i++) {
            diff = data[i+1] - data[i];
            total += diff;
        }
        return (double) total/size;
    }

    public double getSections_start() {
        return sections_start;
    }

    public void setSections_start(double[] sections_start) {
        this.sections_start = getMeanOfArray(sections_start);
        sections_length = sections_start.length;
    }

    public double[] getFeatures(){
        double[] rt = {duration, end_of_fade_in, (double) key, loudness, (double) mode,
            start_of_fade_out, tempo,(double) time_signature,
            (double) year, sections_start, segments_start, tatums_start,
            bars_start, beats_start, segments_loudness_max, (double) segments_length,
            (double) sections_length,(double) bars_length, (double) beats_length};

        double[] rt2 = RowParser.combineDoubles(segments_timbre,segments_pitches);

        return  RowParser.combineDoubles(rt, rt2);
    }

    private double[] findAveragePitches(double[] data){
        double[] rt = new double[12];
        int size = data.length -12;
        double sum;
        for (int i =0; i < size; i++) {
            sum = data[i] + data[i+12];
            rt[i%12] += sum;
        }
        return divideArray(rt, data.length/12);
    }

    private double[] findTimbreDifference(double[] data){
        double[] rt = new double[12];
        int size = data.length -12;
        double diff;
        for (int i =0; i < size; i++) {
            diff = data[i] - data[i+12];
            rt[i%12] += diff;
        }
        return divideArray(rt, data.length/12);
    }

    private double[] divideArray(double[] data, int div){
	for(int i= 0; i < data.length; i++){
	    data[i] = data[i]/div;
	}
	return data;
    }
}
