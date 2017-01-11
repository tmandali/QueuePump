<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:msxsl="urn:schemas-microsoft-com:xslt" exclude-result-prefixes="msxsl"
>
    <xsl:output method="xml" indent="yes"/>

    <xsl:template match="Export">
      <test>
          <xsl:copy>
            <xsl:value-of select="@MessageId"/>
          </xsl:copy>
      </test>
    </xsl:template>
</xsl:stylesheet>
