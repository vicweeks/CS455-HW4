/*
 * This class is used by Beans.encoder to transform the data.
 * Victor Weeks, Diego Batres, Josiah May
 * 
 */


import Util.RowParser;
import java.io.Serializable;

public class Song implements Serializable {
    public String artist_terms;
    public double danceability;
    public double duration;
    public double end_of_fade_in;
    public double energy;
    public int key;
    public double loudness;
    public int mode;
    public double start_of_fade_out;
    public double tempo;
    public int time_signature;
    public int year;
    public double[] segments_start;
    public double[] segments_timbre;
    public double[] tatums_start;
    public double[] bars_start;
    public double[] beats_start;
    public double[] segments_loudness_max;
    public double[] segments_pitches;

    
    public String getArtist_Terms() { return artist_terms; }
    public void setArtist_Terms(String artist_terms) { this.artist_terms = artist_terms; }
    
    public double getDanceability() { return danceability; }
    public void setDanceability(double danceability) { this.danceability = danceability; }
	
    public double getDuration() { return duration; }
    public void setDuration(double duration) { this.duration = duration; }
    
    public double getEnd_Of_Fade_In() { return end_of_fade_in; }
    public void setEnd_Of_Fade_In(double end_of_fade_in) { this.end_of_fade_in = end_of_fade_in; }
	
    public double getEnergy() { return energy; }
    public void setEnergy(double energy) { this.energy = energy; }
    
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

    public double[] getSegments_start() { return segments_start; }

    public void setSegments_start(double[] segments_start) { this.segments_start = segments_start; }

    public double[] getSegments_timbre() {
        return segments_timbre;
    }

    public void setSegments_timbre(double[] segments_timbre) {
        this.segments_timbre = segments_timbre;
    }

    public double[] getTatums_start() {
        return tatums_start;
    }

    public void setTatums_start(double[] tatums_start) {
        this.tatums_start = tatums_start;
    }

    public double[] getBars_start() {
        return bars_start;
    }

    public void setBars_start(double[] bars_start) {
        this.bars_start = bars_start;
    }

    public double[] getBeats_start() {
        return beats_start;
    }

    public void setBeats_start(double[] beats_start) {
        this.beats_start = beats_start;
    }

    public double[] getSegments_loudness_max() {
        return segments_loudness_max;
    }

    public void setSegments_loudness_max(double[] segments_loudness_max) {
        this.segments_loudness_max = segments_loudness_max;
    }

    public double[] getSegments_pitches() {
        return segments_pitches;
    }

    public void setSegments_pitches(double[] segments_pitches) {
        this.segments_pitches = segments_pitches;
    }

    private double getMeanOfArray(double[] data){
        double total = 0.0;
        int size = data.length -1;
        double diff = 0.0;
        for (int i = 0; i < size; i++) {
            diff = data[i+1] - data[i];
            total += diff;
        }
        return total;
    }

}
