/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.mrunit.example;

import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Lucalves33
 */
public class JavaTest {
    
    public JavaTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
     @Test
     public void testArrayCopy() {
        String[] expectedResult = {"1", "2", "3", "4", "5", "6", "7", "8"};
        String sequence = "1,2,3,4,5";
        
        String[] parts = sequence.split(",");
        
        String[] newParts = Arrays.copyOf(parts, parts.length + 3);
        
        newParts[5] = "6";
        newParts[6] = "7";
        newParts[7] = "8";
        
         assertArrayEquals(expectedResult, newParts);
     }
}
