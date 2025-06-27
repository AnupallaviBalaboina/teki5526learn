package dev.gopi.movies;

import org.bson.types.ObjectId;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Review {

    private ObjectId id;

    private String body;
    
    public Review(String body){
        this.body = body;
    }
    
}
