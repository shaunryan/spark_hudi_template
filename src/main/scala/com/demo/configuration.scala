package com.demo
import com.demo.exceptions.EnvVarNotFoundException
import java.nio.file.Paths
import java.nio.file.Path
import org.apache.log4j.{LogManager, Logger}


object Configuration
{
    val log:Logger = LogManager.getRootLogger()

    def getEnv: ((String) => String) = 
    { 
        variable =>
        sys.env.get(variable)
        match{
            case Some(s) => s
            case None => {
                val msg = s"Environment variable '${variable}' not found"
                log.error(msg)
                throw new EnvVarNotFoundException(msg)
            }
        }
    }
    
    def getEnvironment: (() => String) = () => getEnv("ENVIRONMENT")
    def getDataRoot: (() => String) = () => getEnv("DATAROOT")
    
    val isLocalEnvironment:Boolean = getEnvironment() == "local"

    override def toString() = {
        s"""
        |Configuration:
        |-----------------------------------------------
        | Environment = ${Configuration.getEnvironment()}
        | Data Root = ${Configuration.getDataRoot()}
        """.stripMargin
    }
}