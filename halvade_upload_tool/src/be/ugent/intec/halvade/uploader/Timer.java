/*
 * Copyright (C) 2014 ddecap
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package be.ugent.intec.halvade.uploader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author ddecap
 */
public class Timer {
    private long start;
    private long end;
    private static long year = 60*60*24*7*365;
    private static long week = 60*60*24*7;
    private static long day = 60*60*24;
    private static long hour = 60*60;
    private static long minute = 60;
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    
    
    public static String getGlobalTime() {
        return dateFormat.format(new Date());
    }
    
    public void start(){
        start = System.nanoTime();
        end = 0;
    }
    
    public void stop(){
        end = System.nanoTime();
    }
    
    public String getFormattedElapsedTime(){
        return timeToString(getElapsedTime());
    }
    
    public String getFormattedCurrentTime(){
        return timeToString(getCurrentTime());
    }
    
    public double getElapsedTime(){
        return (double)(end - start) / 1000000000;
    }
    
    public double getCurrentTime(){
        return (double)(System.nanoTime() - start) / 1000000000;
    }

    @Override
    public String toString() {
        if(end == 0)
            return getFormattedCurrentTime();
        else
            return getFormattedElapsedTime();
    }
    
    
    
    private String timeToString(double time)
    {
        String formattedTime = "";
        int precision = 0;
        if (time >= year && precision < 2){
            formattedTime += (int)(time / year) + "y ";
            precision++;
        }
        time = time % year;
        if (time >= week && precision < 2){
            formattedTime += (int)(time / week) + "w ";
            precision++;
        }
        time = time % week;
        if (time >= day && precision < 2){
            formattedTime += (int)(time / day) + "d ";
            precision++;
        }
        time = time % day;
        if (time >= hour && precision < 2){
            formattedTime += (int)(time / hour) + "h ";
            precision++;
        }
        time = time % hour;
        if (time >= minute && precision < 2){
            formattedTime += (int)(time / minute) + "m ";
            precision++;
        }
        time = time % minute;
        if (time >= 1 && precision < 2){
            formattedTime += (int)time + "s ";
            precision++;
        }
        time = (time % 1)*1000;
        if (time >= 1 && precision < 2){
            formattedTime += (int)time + "ms ";
            precision++;
        }
        time = (time % 1)*1000;
        if (time >= 1 && precision < 2){
            formattedTime += (int)time + "μs ";
            precision++;
        }
        time = (time % 1)*1000;
        if (time >= 1 && precision < 2){
            formattedTime += (int)time + "ns ";
        }
        return formattedTime;
    }
}
