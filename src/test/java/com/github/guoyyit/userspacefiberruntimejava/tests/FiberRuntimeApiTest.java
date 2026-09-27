package com.github.guoyyit.userspacefiberruntimejava.tests;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** End-to-end: the fiber runtime serves requests inside the Spring process. */
@SpringBootTest
@AutoConfigureMockMvc
class FiberRuntimeApiTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void spawnPollAndComplete() throws Exception {
        MvcResult created = mvc.perform(post("/api/fibers")
                        .param("name", "api-demo")
                        .param("steps", "3")
                        .param("sleepMs", "30"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("api-demo"))
                .andReturn();
        long id = ((Number) JsonPath.read(created.getResponse().getContentAsString(), "$.id")).longValue();

        String state = pollUntilTerminal(id);
        assertEquals("COMPLETED", state);

        MvcResult done = mvc.perform(get("/api/fibers/{id}", id))
                .andExpect(status().isOk())
                .andReturn();
        String outcome = JsonPath.read(done.getResponse().getContentAsString(), "$.outcome");
        assertTrue(outcome.contains("finished 3 steps"), outcome);
    }

    @Test
    void spawnAndCancel() throws Exception {
        MvcResult created = mvc.perform(post("/api/fibers")
                        .param("name", "to-cancel")
                        .param("steps", "1000")
                        .param("sleepMs", "1000"))
                .andExpect(status().isOk())
                .andReturn();
        long id = ((Number) JsonPath.read(created.getResponse().getContentAsString(), "$.id")).longValue();

        mvc.perform(delete("/api/fibers/{id}", id)).andExpect(status().isOk());
        assertEquals("CANCELLED", pollUntilTerminal(id));
    }

    private String pollUntilTerminal(long id) throws Exception {
        for (int i = 0; i < 200; i++) {
            MvcResult res = mvc.perform(get("/api/fibers/{id}", id))
                    .andExpect(status().isOk())
                    .andReturn();
            String state = JsonPath.read(res.getResponse().getContentAsString(), "$.state");
            if (state.equals("COMPLETED") || state.equals("FAILED") || state.equals("CANCELLED")) {
                return state;
            }
            Thread.sleep(25);
        }
        fail("fiber " + id + " did not terminate in time");
        return null;
    }
}
